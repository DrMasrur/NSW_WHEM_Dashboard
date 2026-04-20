# WHEM-AI-Dashboard

Streamlit dashboard for WHEM direct emissions and related visualisations.

Quick start (remote / SDC):

1. Activate your project environment (create one if needed):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

2. Run the app (bind to localhost for VS Code port forwarding):

```bash
streamlit run direct_emission_streamlit_sdc.py --server.address 127.0.0.1 --server.port 8501
```

3. Use VS Code Remote-SSH port forwarding or an SSH tunnel to open the app locally:

```bash
# local machine
ssh -L 8501:127.0.0.1:8501 your-sdc-login
open http://127.0.0.1:8501
```

Notes:
- The app depends on geospatial libraries (`geopandas`, `fiona`, `gdal`) which often need system packages (GDAL). If installation fails on a clean environment, prefer running on a VM/host with system GDAL or use Docker.
- Streamlit Community Cloud may fail to build this repo because of these native dependencies.

Files:
- `direct_emission_streamlit_sdc.py` — main Streamlit app
- `direct_emission.py` — helper utilities
- `requirements.txt` — Python package requirements

If you want, I can create a `Dockerfile` to simplify deployment with GDAL installed.