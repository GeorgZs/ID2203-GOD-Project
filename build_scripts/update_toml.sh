set -u

# Parse key-value arguments
declare -A args
while [[ "$#" > "0" ]]; do
  case "$1" in 
    (*=*)
        _key="${1%%=*}" &&  _key="${_key/--/}" && _val="${1#*=}"
        args[${_key}]="${_val}"
        ;;
  esac
  shift
done

if [ -z "${args[*]}" ]; then
  echo "Usage: update_toml <toml-key-1>=<toml-value-1> [ <toml-key-2>=<toml-value-2> ]"
  echo "Surround values in quotes if they contain spaces and use value 'None' to comment out key"
  echo "If values should be strings then you must escape the quotes with \\\""
fi

# For each key-value update the TOML keys with the value, or if value is "None" comment out the key
for key in "${!args[@]}"; do
  value="${args[$key]}"
  echo "Key: $key, Value: $value"
  for file in ./*.toml; do
    if [[ -f $file ]]; then
      if ! grep -q "^#\?$key = .*" "$file"; then
        echo "WARNING: Key $key does not exist in file $file"
      fi
      if [[ "$value" == "None" ]]; then
        sed -i "s/^\($key = .*\)/#\1/" "$file"
      else
        sed -i "s/^#\?$key = .*/$key = $value/" "$file"
      fi
    fi
  done
done
