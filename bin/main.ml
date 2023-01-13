let usage_msg = "littlejane -c <csv file> [-o <result file>] <program name>"
let args = ref []
let csv_file = ref ""
let parallelism = ref 4
let output_file = ref ""
let anon_fun arg = args := !args @ [arg]
let speclist = [
  ("-c", Arg.Set_string csv_file, "CSV arguments file name");
  ("-j", Arg.Set_int parallelism, "Amount of concurrency");
  ("-o", Arg.Set_string output_file, "Optional file to save results to")
]

exception Invalid_csv_file
exception Program_name_missing

let parse_args (arglist : string list) : string * string list =
  let progname = ref "" in
    let direct_args = ref [] in
      let () = match arglist with
        | [] -> raise Program_name_missing
        | "--" :: _ -> raise Program_name_missing
        | name :: _ -> progname := name
      in
        let full_direct_args = !progname :: !direct_args in
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


let () =
  Arg.parse speclist anon_fun usage_msg;
  let progname, direct_args = parse_args !args in
      try
        let headers, rows = load_csv !csv_file in
          let oc =
            match !output_file with
              | "" -> Stdlib.stdout
              | _ -> open_out !output_file
          in
            List.iter (fun row -> 
              let arglist = build_command_args headers row direct_args in
                let ic = Unix.open_process_args_in progname (Array.of_list arglist) in
                  let rec loop (i) =
                    let line = input_line ic in
                      Printf.fprintf oc "%s, %d, %s\n" (String.concat ", " arglist) i line;
                    loop (i + 1)
                  in
                    try
                      loop (1)
                    with 
                    | End_of_file -> close_in ic
            ) rows
      with 
      | Unix.Unix_error(Unix.ENOENT, _, prog) -> Printf.fprintf Stdlib.stderr "Failed to open %s\n" prog
      | Sys_error(reason) -> Printf.fprintf Stdlib.stderr "Failed to read CSV %s\n" reason
      | e -> let reason = Printexc.to_string e in
          Printf.fprintf Stdlib.stderr "Unexpected error: %s\n" reason
  