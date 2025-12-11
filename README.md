# Georgia House Flip Detector 🏠

A dashboard to identify house flips in Georgia (30-365 day holds, $70k-$150k profit).

## Features
- Scrape public deed data from Georgia county websites
- Identify properties bought and sold within 30-365 days
- Filter for $70k-$150k profit margins
- Generate investor contact lists
- Simple web dashboard

## Quick Start
```bash
# Clone and install
git clone https://github.com/yourusername/georgia-house-flip-detector.git
cd georgia-house-flip-detector
pip install -r requirements.txt

# Run the pipeline
python scripts/run_pipeline.py

# Start the dashboard
python dashboard/app.py
