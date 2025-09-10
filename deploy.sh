#!/bin/bash

set -euo pipefail

###############################################
# Baggage Handling Simulator Deployment Script
# Adds CLI arguments for non-interactive usage.
# Any value not supplied via arguments will fall
# back to interactive prompts (except when --yes
# / --non-interactive is provided, in which case
# required values must be passed or the script fails).
###############################################

AppName="bhsim-app"
ResourceGroup="bhsim-rg"
Location=""
RegistryName=""
ImageName="bhsim"
ConnectionStringSecret="" # Event Hubs connection string (may include EntityPath)
EventHubName=""            # Optional explicit Event Hub name if not in connection string
SqlConnectionString=""     # Optional SQL connection string
NonInteractive=0

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
    --app-name NAME                 Container instance name (default: bhsim-app)
    --resource-group NAME           Azure resource group name (default: bhsim-rg)
    --location LOCATION             Azure region (e.g. westeurope)
    --registry NAME                 Existing Azure Container Registry name (no fqdn)
    --image NAME                    Image name inside registry (default: bhsim)
    --eventhub-connection STRING    Event Hubs connection string (may contain EntityPath)
    --eventhub-name NAME            Event Hub name (if not part of connection string)
    --sql-connection STRING         Optional SQL Server ODBC connection string
    --non-interactive | -y          Fail instead of prompting for missing required inputs
    -h | --help                     Show this help

Examples:
    $0 --location westeurope --registry myacr \\
         --eventhub-connection "Endpoint=...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=myehub" \\
         --resource-group bhsim-rg --app-name bhsim-demo

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --app-name) AppName="$2"; shift 2;;
        --resource-group) ResourceGroup="$2"; shift 2;;
        --location) Location="$2"; shift 2;;
        --registry) RegistryName="$2"; shift 2;;
        --image) ImageName="$2"; shift 2;;
        --eventhub-connection|--eventhub-connection-string) ConnectionStringSecret="$2"; shift 2;;
        --eventhub-name) EventHubName="$2"; shift 2;;
        --sql-connection|--sql-connection-string) SqlConnectionString="$2"; shift 2;;
        --non-interactive|-y|--yes) NonInteractive=1; shift;;
        -h|--help) usage; exit 0;;
        *) echo "Unknown argument: $1" >&2; usage; exit 1;;
    esac
done

if [[ $NonInteractive -eq 0 ]]; then
    # prompt to change or leave the $AppName
    read -p $'\nDefault app name is '"$AppName"$'.\nProvide a new name or press ENTER to keep the current name: ' changeAppName || true
    if [[ -n "${changeAppName:-}" ]]; then AppName=$changeAppName; fi
fi

if [[ -z "$ConnectionStringSecret" && $NonInteractive -eq 0 ]]; then
    read -p $'\nProvide the Azure Event Hubs connection string (may include EntityPath): ' ConnectionStringSecret || true
fi

if [[ -z "$ConnectionStringSecret" ]]; then
    echo "Event Hubs connection string is required." >&2
    exit 1
fi

# Basic validation: must contain Endpoint and SharedAccessKey
if [[ ! "$ConnectionStringSecret" =~ Endpoint=.*SharedAccessKey=.* ]]; then
    echo "Connection string appears invalid (missing Endpoint or SharedAccessKey)." >&2
    exit 1
fi

# Try to extract EntityPath if not passed separately
if [[ -z "$EventHubName" ]]; then
    if [[ "$ConnectionStringSecret" =~ EntityPath=([^;]+) ]]; then
        EventHubName="${BASH_REMATCH[1]}"
    fi
fi

# Login to Azure if not already logged in
if ! az account show >/dev/null; then
    az login
fi

if [[ -z "$Location" ]]; then
    if [[ $NonInteractive -eq 1 ]]; then
        echo "--location is required in non-interactive mode." >&2
        exit 1
    fi
    locations=$(az account list-locations --query "[?metadata.regionType == 'Physical'].{Name:name, DisplayName:displayName, Geo: metadata.geographyGroup}" --output tsv | awk '{print NR" "$1" "$2" "$3}')
    echo -e "\nAvailable Locations:" >&2
    echo "$locations" | while read -r line; do
        idx=$(echo "$line" | awk '{print $1}')
        name=$(echo "$line" | awk '{print $2}')
        echo "$idx) $name" >&2
    done
    read -p $'Enter the number of the desired location: ' locationNumber || true
    if [[ -z "$locationNumber" || ! "$locationNumber" =~ ^[0-9]+$ ]]; then
        echo "Invalid location selection." >&2; exit 1;
    fi
    Location=$(echo "$locations" | awk -v choice="$locationNumber" 'NR==choice {print $2}')
fi

# Prompt for the ResourceGroup if empty and validate input
while true; do
    read -p $'\nResource group is "$ResourceGroup".\nProvide a new name or press Enter to keep the current name: ' changeResourceGroup
    if [[ -z "$changeResourceGroup" ]]; then
        break
    fi
    if [[ "$changeResourceGroup" =~ ^[a-zA-Z0-9_-]+$ ]]; then
        ResourceGroup=$changeResourceGroup
        break
    fi
done

if [[ -z "$RegistryName" ]]; then
    if [[ $NonInteractive -eq 1 ]]; then
        echo "--registry is required in non-interactive mode." >&2
        exit 1
    fi
    while true; do
        read -p $'\nEnter the Azure Container Registry name: ' RegistryName || true
        if [[ -n "$RegistryName" && "$RegistryName" =~ ^[a-zA-Z0-9_-]+$ ]]; then break; fi
    done
fi

# Check if the resource group already exists
existingResourceGroup=$(az group show --name $ResourceGroup --query name --output tsv)
# Create the resource group if it doesn't exist
if [[ -z "$existingResourceGroup" ]]; then
    az group create --name $ResourceGroup --location $Location
fi

AcrName="$RegistryName.azurecr.io"
existingRegistry=$(az acr show --name "$RegistryName" --query name --output tsv || true)
if [[ -z "$existingRegistry" ]]; then
    echo "Container Registry '$RegistryName' not found (script does not create it)." >&2
    exit 1
fi

# Log in to the Azure Container Registry
az acr login --name $RegistryName

registryPassword=$(az acr credential show --name "$RegistryName" --query passwords[0].value --output tsv)
registryUsername=$(az acr credential show --name "$RegistryName" --query username --output tsv)

existingContainer=$(az container show --resource-group "$ResourceGroup" --name "$AppName" --query name --output tsv 2>/dev/null || true)
createArgs=(
    --resource-group "$ResourceGroup"
    --name "$AppName"
    --image "$AcrName/${ImageName}:latest"
    --cpu 1
    --memory 2
    --restart-policy Always
    --secure-environment-variables EVENTHUB_CONNECTION_STRING="$ConnectionStringSecret"
    --registry-password "$registryPassword"
    --registry-username "$registryUsername"
)
if [[ -n "$EventHubName" ]]; then
    createArgs+=( --environment-variables EVENTHUB_NAME="$EventHubName" )
fi
if [[ -n "$SqlConnectionString" ]]; then
    createArgs+=( --secure-environment-variables SQLSERVER_CONNECTION_STRING="$SqlConnectionString" )
fi

if [[ -z "$existingContainer" ]]; then
    az container create "${createArgs[@]}"
else
    az container restart --resource-group "$ResourceGroup" --name "$AppName"
fi

echo "Deployment complete."
