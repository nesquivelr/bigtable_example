# Create environment using conda
```bash
conda create -n bt python=3.11
conda activate bt
pip install google-cloud-bigtable==2.17.0
pip install bson==0.5.10
```

# Run this command in one terminal
```bash
gcloud beta emulators bigtable start --host-port="0.0.0.0:8086"
```

# Run this command in another terminal
```bash
python main.py
```
