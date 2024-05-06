 #!/bin/bash

input_file="$1"

if [[ ! -f "$input_file" ]]; then
  echo "Input file $input_file not found."
  exit 1
fi

output_file="outputmetadata.txt"

if [[ -f "$output_file" ]]; then
  > "$output_file"  
fi

while IFS=',' read -r _ Symbol Security_Name _ _ _ _ _ _ _ _ _; do
  if [[ "$Symbol" == "Symbol" ]]; then
    continue
  fi

  if [[ -z "${Symbol// }" || -z "${Security_Name// }" ]]; then
    continue
  fi

   if [[ ${Security_Name:0:1} == "\"" ]]; then
    txt="$Symbol:{\"Symbol\": \"$Symbol\",\"SecurityName\": $Security_Name\"}"
  else
    txt="$Symbol:{\"Symbol\": \"$Symbol\",\"SecurityName\": \"$Security_Name\"}"
  fi

  
  echo "$txt" >> "$output_file"
done < "$input_file"
echo  "$output_file"


