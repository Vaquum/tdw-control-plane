#!/bin/bash

echo "Setting up Jupyter shortcuts..."

# Get Jupyter paths
JUPYTER_CONFIG_DIR=$(python -c "from jupyter_core.paths import jupyter_config_dir; print(jupyter_config_dir())")
JUPYTER_DATA_DIR=$(python -c "from jupyter_core.paths import jupyter_data_dir; print(jupyter_data_dir())")

echo "Jupyter config dir: $JUPYTER_CONFIG_DIR"
echo "Jupyter data dir: $JUPYTER_DATA_DIR"

# For JupyterLab user settings, we need to construct the path
# JupyterLab stores user settings in: {jupyter_config_dir}/lab/user-settings/
LAB_SETTINGS_DIR="$JUPYTER_CONFIG_DIR/lab/user-settings/@jupyterlab/shortcuts-extension"

echo "Creating shortcuts directory: $LAB_SETTINGS_DIR"
mkdir -p "$LAB_SETTINGS_DIR"

# Create shortcuts configuration
cat > "$LAB_SETTINGS_DIR/shortcuts.jupyterlab-settings" << 'EOF'
{
    "shortcuts": [
        {
            "command": "notebook:run-all-cells",
            "keys": ["Ctrl F9"],
            "selector": ".jp-Notebook:focus"
        },
        {
            "command": "notebook:restart-run-all",
            "keys": ["Ctrl Shift R"],
            "selector": ".jp-Notebook:focus"
        }
    ]
}
EOF

echo "Shortcuts configuration created at: $LAB_SETTINGS_DIR/shortcuts.jupyterlab-settings"

# Verify the file was created
if [ -f "$LAB_SETTINGS_DIR/shortcuts.jupyterlab-settings" ]; then
    echo "✓ Shortcuts file created successfully"
    echo "File contents:"
    cat "$LAB_SETTINGS_DIR/shortcuts.jupyterlab-settings"
else
    echo "✗ Failed to create shortcuts file"
fi

echo "Starting Jupyter Lab..."

# Start Jupyter Lab
exec jupyter lab --ip=0.0.0.0 --no-browser --allow-root \
  --NotebookApp.token='1342084237095993446/1342084357812260895' \
  --NotebookApp.password=''