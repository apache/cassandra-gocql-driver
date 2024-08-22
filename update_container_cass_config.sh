#!/bin/bash

# Path to the cassandra.yaml file inside the container
CASSANDRA_CONFIG="/etc/cassandra/cassandra.yaml"

# Function to update a property in the cassandra.yaml file
update_property() {
  local property=$1
  local value=$2
  local root_property=${property%%.*}
  local nested_property=${property#*.}

  local indent=""
  if [[ $CASS_VERSION == 4.0.* ]]; then
    indent="    "
  elif [[ $CASS_VERSION == 4.1.* ]]; then
    indent="  "
  else
    indent="    "
  fi

  if grep -q "^${property}:" "$CASSANDRA_CONFIG"; then
    # If the property exists, update its value
    sed -i "s|^\(${property}:\).*|\1 ${value}|" "$CASSANDRA_CONFIG"
#    echo "Updated $property to $value"
  else
    if [[ "$property" == *"."* ]]; then
      # If it's a nested property
      if grep -q "^${root_property}:" "$CASSANDRA_CONFIG"; then
        if grep -q "^${indent}# ${nested_property}:" "$CASSANDRA_CONFIG"; then
         # Check if the nested property is commented out
         sed -i "/^${root_property}:/,/^[^ ]/ s|^\(${indent}# ${nested_property}:\).*|${indent}${nested_property}: ${value}|" "$CASSANDRA_CONFIG"
        elif grep -q "^${indent}${nested_property}:" "$CASSANDRA_CONFIG"; then
        echo "Added nested_property $nested_property  into root_property $root_property with value $value"
          sed -i "/^${root_property}:/,/^[^ ]/ s|^\(${indent}${nested_property}:\).*|\1 ${value}|" "$CASSANDRA_CONFIG"
        else
          # Add nested property under existing root property
          awk -v root="$root_property" -v prop="$nested_property" -v val="$value" -v ind="$indent" '
          $0 ~ "^"root":" {
            print $0
            print ind prop": "val
            next
          }
          { print $0 }
          ' "$CASSANDRA_CONFIG" > tmpfile && mv tmpfile "$CASSANDRA_CONFIG"
        fi
      else
        # Add new root property with nested property
        echo -e "${root_property}:\n${indent}${nested_property}: ${value}" >> "$CASSANDRA_CONFIG"
      fi
    else
      # If it's a root-level property, add it directly
      echo "${property}: ${value}" >> "$CASSANDRA_CONFIG"
    fi
  fi
}

# Function to configure Cassandra based on the version
configure_cassandra() {
  local keypath="testdata"
  local conf=(
    "concurrent_reads:2"
    "concurrent_writes:2"
  )

  if [[ $AUTH_TEST == true ]]; then
    conf+=(
      "authenticator: PasswordAuthenticator"
      "authorizer: CassandraAuthorizer"
        )
  fi

  if [[ $RUN_SSL_TEST == true ]]; then
    conf+=(
      "client_encryption_options.enabled:true"
      "client_encryption_options.keystore:$keypath/.keystore"
      "client_encryption_options.keystore_password:cassandra"
      "client_encryption_options.require_client_auth:true"
      "client_encryption_options.truststore:$keypath/.truststore"
      "client_encryption_options.truststore_password:cassandra"
        )
  fi

  if [[ $CASS_VERSION == 3.*.* ]]; then
    conf+=(
      "rpc_server_type:sync"
      "rpc_min_threads:2"
      "rpc_max_threads:2"
      "enable_user_defined_functions:true"
      "enable_materialized_views:true"
      "write_request_timeout_in_ms:5000"
      "read_request_timeout_in_ms:5000"
    )
  elif [[ $CASS_VERSION == 4.0.* ]]; then
    conf+=(
      "enable_user_defined_functions:true"
      "enable_materialized_views:true"
      "write_request_timeout_in_ms:5000"
      "read_request_timeout_in_ms:5000"
    )
  else
    conf+=(
      "user_defined_functions_enabled:true"
      "materialized_views_enabled:true"
      "write_request_timeout:5000ms"
      "read_request_timeout:5000ms"
    )
  fi

  for setting in "${conf[@]}"; do
    IFS=":" read -r property value <<< "$setting"
    update_property "$property" "$value"
  done
}

# update Cassandra config
configure_cassandra

# Update rpc addresses with the container's IP address
IP_ADDRESS=$(hostname -i)
sed -i "s/^rpc_address:.*/rpc_address: $IP_ADDRESS/" /etc/cassandra/cassandra.yaml
sed -i "s/^# broadcast_rpc_address:.*/broadcast_rpc_address: $IP_ADDRESS/" /etc/cassandra/cassandra.yaml

echo "Cassandra configuration modified successfully."

