#/bin/bash
source /work/aheidelbach/git/b2luigiB2/venv/bin/activate

echo "Done!"
pip3 show b2luigi
python3 -c "import b2luigi;print(b2luigi.__file__)"
