
#!/bin/bash

input_file="$1"

if [[ ! -f "$input_file" ]]; then
  echo "Input file $input_file not found."
  exit 1
fi

output_file="output.txt"

while IFS=',' read -r Date Open High Low Close _ Volume Stock; do
  # Skip the header line
  if [[ "$Date" == "Date" ]]; then
    continue
  fi


  if [[ -z "${Date// }" || -z "${Open// }" || -z "${High// }" || -z "${Low// }" || -z "${Close// }" || -z "${Volume// }" || -z "$Stock" ]]; then
    continue
  fi

  txt="$Stock:{\"Stock\": \"$Stock\",\"Date\": \"$Date\",\"Open\": \"$Open\",\"High\": \"$High\",\"Low\": \"$Low\",\"Close\": \"$Close\",\"Volume\": \"$Volume\"}"
  
  echo "$txt" >> "$output_file"
done < "$input_file"
echo  "$output_file"
