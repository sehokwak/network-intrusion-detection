# network-intrusion-detection

Extracts and compares egonet-based features in network traffic data to detect anomalous nodes.

### Version Information

network-intrusion-detection was written in **Scala 2.12.10** using **Apache Spark GraphX 3.1.2**.

## Usage

A modified version of the KDD Cup 99 dataset is used, which can be found [here](https://www.unb.ca/cic/datasets/nsl.html).

To run network-intrusion-detection:
1. Name the KDD dataset textfile to `input.txt`

2. In `runme.bash`, specify the path to the directory where Spark is installed, the number of samples, and the k-value. It is set to a sample size of 1000 and a k-value of 40 by default.

3. Some configurations may be needed depending on how you run the application. For example, it is currently set to run locally on one node:
  ```
  /pathToSparkDirectory/bin/spark-submit \
    --class featureExtraction \
    --master local[*] \
  ```
  More information on submitting applications on different types of clusters can be found [here](https://spark.apache.org/docs/latest/submitting-applications.html).

4. Run the `runme.bash` script:
```
bash runme.bash
```

*This project was conducted in summer 2021 by Nathan Paik and Se Ho Kwak under the mentorship of Dr. Enyue Lu at Salisbury University. This work was funded by the National Science Foundation's Research Experience for Undergraduates.*
