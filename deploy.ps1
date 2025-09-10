# PowerShell version: 7.1

# Define variables (defaults can be overridden interactively)
$AppName = "bhsim-app"
$ResourceGroup = "bhsim-rg"
$Location = ""
$RegistryName = ""
$ImageName = "bhsim"
$EventHubConnectionString = ""
$EventHubName = ""
$SqlConnectionString = ""

# prompt to change or leave the $AppName
$changeAppName = Read-Host "`r`nDefault app name is '$AppName'.`r`nProvide a new name or press ENTER to keep the current name"
if (-not [string]::IsNullOrEmpty($changeAppName)) {
    $AppName = $changeAppName
}

# Prompt for Event Hub connection string
if ([string]::IsNullOrEmpty($EventHubConnectionString)) {
    $EventHubConnectionString = Read-Host "`r`nProvide the Azure Event Hubs connection string (may include EntityPath)"
}
if ([string]::IsNullOrEmpty($EventHubConnectionString)) { Write-Host "Event Hubs connection string required."; exit }

# Try to extract Event Hub name if not provided
if ([string]::IsNullOrEmpty($EventHubName)) {
    if ($EventHubConnectionString -match "EntityPath=([^;]+)") { $EventHubName = $Matches[1] }
}
if ([string]::IsNullOrEmpty($EventHubName)) { $EventHubName = Read-Host "Enter Event Hub name (if not in connection string)" }

# Optional SQL connection string
if ([string]::IsNullOrEmpty($SqlConnectionString)) {
    $SqlConnectionString = Read-Host "(Optional) Provide SQL Server ODBC connection string or press ENTER to skip"
}

# Login to Azure if not already logged in
if (-not (az account show)) {
    az login
}

# Prompt for the Location from a list of available locations
$locations = az account list-locations --query "[?metadata.regionType == 'Physical'].{Name:name, DisplayName:displayName, Geo: metadata.geographyGroup}" --output json | ConvertFrom-Json | Sort-Object Geo, DisplayName
$locationChoice = $null
do {
    Write-Host "`r`nAvailable Locations:"
    $priorGeo = ""
    $locations | ForEach-Object {
        $index = [array]::IndexOf($locations, $_)
        $number = $index + 1
        if ($priorGeo -ne $_.Geo) {
            Write-Host
            Write-Host
            Write-Host $_.Geo
            Write-Host "------------------"
            $priorGeo = $_.Geo
        }
        $displayName = $_.DisplayName
        Write-Host -NoNewLine ("{0,-30}" -f "$number. $displayName")
        if ($index % 3 -eq 2) {
            Write-Host
        }
    }
    $locationNumber = Read-Host "`r`nEnter the number of the desired location"
    if ([int]::TryParse($locationNumber, [ref]$locationChoice) -and $locationChoice -ge 1 -and $locationChoice -le $locations.Count+1) {
        $locationChoice -= 1
    } else {
        Write-Host "Invalid input. Please enter a valid number."
    }
} while ($null -eq $locationChoice)
$Location = $locations[$locationChoice].Name


# Prompt for the ResourceGroup if empty and validate input
do {
    $changeResourceGroup = Read-Host "`r`nResource group is '$ResourceGroup'.`r`nProvide a new name or press Enter to keep the current name"
    if ([string]::IsNullOrEmpty($changeResourceGroup)) {
        break
    }
} while (-not ($changeResourceGroup -match "^[a-zA-Z0-9_-]+$"))
if (-not [string]::IsNullOrEmpty($changeResourceGroup)) {
    $ResourceGroup = $changeResourceGroup
}


#Prompt for the RegistryName if empty and validate input
if ([string]::IsNullOrEmpty($RegistryName)) {
    do {
        $RegistryName = Read-Host "`r`nEnter the Azure Container Registry name"
    } while (-not ($RegistryName -match "^[a-zA-Z0-9_-]+$"))
}

# Check if the resource group already exists
$existingResourceGroup = az group show --name $ResourceGroup --query name --output tsv
# Create the resource group if it doesn't exist
if (-not $existingResourceGroup) {
    az group create --name $ResourceGroup --location $Location
}

$AcrName = "$RegistryName.azurecr.io"
# Create a container registry if it doesn't exist
$existingRegistry = az acr show --name $RegistryName --query name --output tsv
if (-not $existingRegistry) {
    Write-Host "Exiting."
    exit
}

# Log in to the Azure Container Registry
az acr login --name $RegistryName


$registryPassword = az acr credential show --name $RegistryName --query passwords[0].value --output tsv
$registryUsername = az acr credential show --name $RegistryName --query username --output tsv

$existingContainer = az container show --resource-group $ResourceGroup --name $AppName --query name --output tsv
if  (-not $existingContainer) {
    $envArgs = @(
        "--secure-environment-variables", "EVENTHUB_CONNECTION_STRING=$EventHubConnectionString"
    )
    if ($EventHubName) { $envArgs += @("--environment-variables", "EVENTHUB_NAME=$EventHubName") }
    if ($SqlConnectionString) { $envArgs += @("--secure-environment-variables", "SQLSERVER_CONNECTION_STRING=$SqlConnectionString") }

    az container create --resource-group $ResourceGroup `
        --name $AppName `
        --image "$AcrName/${ImageName}:latest" `
        --cpu 1 `
        --memory 2 `
    --restart-policy Always `
    @envArgs `
        --registry-password $registryPassword `
        --registry-username $registryUsername
} else {
    az container restart --resource-group $ResourceGroup --name $AppName
}

