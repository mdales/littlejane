module Chan = Domainslib.Chan

type 'a message = Task of 'a | Quit

let usage_msg = "littlejane -c <csv file> [-o <result file>] <program name>"
let args = ref []
let pass_through_args = ref []
let csv_file = ref ""
let parallelism = ref 4
let output_file = ref ""
let anon_fun arg = args := !args @ [arg]
let rest_fun arg = pass_through_args := !pass_through_args @ [arg]
let speclist = [
  ("-c", Arg.Set_string csv_file, "CSV arguments file name");
  ("-j", Arg.Set_int parallelism, "Amount of concurrency");
  ("-o", Arg.Set_string output_file, "Optional file to save results to");
  ("--", Arg.Rest rest_fun, "Other args passed directly to child")
]

exception Invalid_csv_file
exception Program_name_missing

let parse_args (arglist : string list) (direct_arg_list : string list): string * string list =
  let progname = ref "" in
    let () = match arglist with
      | [] -> raise Program_name_missing
      | name :: _ -> progname := name
    in
      let full_direct_args = !progname :: direct_arg_list in
        !progname, full_direct_args

let load_csv (filename : string) : string list * string list list =
  let headers = ref [] in
    let rows = ref [] in
      let ic = open_in filename in
        let () = try
          let cc = Csv.of_channel ic in
            headers := Csv.next cc;
            let element_count = List.length !headers in
              let rec loop() = 
                let row = Csv.next cc in
                  let row_count = List.length row in
                    if row_count == element_count then
                      rows := row :: !rows
                    else
                      raise Invalid_csv_file 
                  ;
                  loop()
              in loop()
        with 
          | End_of_file -> close_in ic
          | e -> close_in_noerr ic; raise e 
        in
          !headers, List.rev !rows

let build_command_args (headers : string list) (row : string list) (direct : string list) : string list =
  let args = ref [] in
    List.iter2 ( fun h r ->
      args := r :: h :: !args
    ) headers row
    ;
    direct @ List.rev !args

let run_instance (arglist : string list) (outputQ : 'b Chan.t) : unit =
  let arrayargs = Array.of_list arglist in
    let ic = Unix.open_process_args_in arrayargs.(0) arrayargs in
      let rec loop (i) =
        let line = input_line ic in
          let res = Printf.sprintf "%s, %d, %s" (String.concat ", " arglist) i line in
            Chan.send outputQ (Task res);
            loop (i + 1)
      in
        try
          loop (1)
        with 
        | End_of_file -> close_in ic

let rec worker (inputQ: 'a Chan.t) (outputQ: 'b Chan.t) : unit =
  match Chan.recv inputQ with
  | Task row ->
      run_instance row outputQ;
      worker inputQ outputQ 
  | Quit -> Chan.send outputQ Quit

let output_consumer (outputQ : 'b Chan.t) (producer_count : int) : unit = 
  let oc =
    match !output_file with
      | "" -> Stdlib.stdout
      | _ -> open_out !output_file
  in
      let rec loop (counter : int) : unit = 
        let () = match Chan.recv outputQ with
          | Task result -> Printf.fprintf oc "%s\n" result; loop(counter)
          | Quit -> 
            let decremented_counter = (counter - 1) in
              match decremented_counter with
              | 0 -> ()
              | _ -> loop decremented_counter
        in ();
      in loop producer_count

let () =
  Arg.parse speclist anon_fun usage_msg;
  let _, direct_args = parse_args !args !pass_through_args in
    let inputQ = Chan.make_unbounded () in
      let outputQ = Chan.make_unbounded () in
        try
          let headers, rows = load_csv !csv_file in
            List.iter (fun row ->
              let arglist = build_command_args headers row direct_args in
                Chan.send inputQ (Task arglist)
            ) rows;
            for _ = 1 to !parallelism do
              Chan.send inputQ Quit
            done
            ;
            let sinkDomain = Domain.spawn(fun _ -> output_consumer outputQ !parallelism) in
              let domains = Array.init !parallelism
                (fun _ -> Domain.spawn(fun _ -> worker inputQ outputQ)) in
              Array.iter Domain.join domains;
              Domain.join sinkDomain
        with 
        | Unix.Unix_error(Unix.ENOENT, _, prog) -> Printf.fprintf Stdlib.stderr "Failed to open %s\n" prog
        | Sys_error(reason) -> Printf.fprintf Stdlib.stderr "Failed to read CSV %s\n" reason
        | e -> let reason = Printexc.to_string e in
            Printf.fprintf Stdlib.stderr "Unexpected error: %s\n" reason
  