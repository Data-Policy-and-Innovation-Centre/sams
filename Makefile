.PHONY: download

# Variables
PYTHON=python
DOWNLOAD_SCRIPT=download_from_box.py
BOX_FILE_ID=YOUR_BOX_FILE_ID
DOWNLOAD_PATH=path/to/save/downloaded_file.ext

# Download a file from Box
download:
	@echo "Downloading file from Box..."
	BOX_CLIENT_ID=$$BOX_CLIENT_ID BOX_CLIENT_SECRET=$$BOX_CLIENT_SECRET BOX_ACCESS_TOKEN=$$BOX_ACCESS_TOKEN $(PYTHON) $(DOWNLOAD_SCRIPT) $(BOX_FILE_ID) $(DOWNLOAD_PATH)
