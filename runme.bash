#!/bin/bash

python filter.py input.txt filtered.txt 1000 # TODO set number of samples here
mkdir temp_output
mkdir feature_comparisons
/pathToSparkDirectory/bin/spark-submit \
 --class egonetFeatures.featureExtraction \
 --master local[*] \
 networkintrusion_2.12-0.1.jar filtered.txt temp_output/ 40 # TODO set k value here
mv temp_output/WeightvsEdge/part-00000 feature_comparisons/WvE.txt
mv temp_output/EigenvsWeight/part-00000 feature_comparisons/PvW.txt
mv temp_output/EigenvsEdge/part-00000 feature_comparisons/PvE.txt
python accuracy.py feature_comparisons/WvE.txt
python accuracy.py feature_comparisons/PvW.txt
python accuracy.py feature_comparisons/PvE.txt
rm -rf temp_output
