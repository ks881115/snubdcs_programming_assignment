K.S. Lee's Linear Logistic Regression implementation on REEF

This project is dependent on Shimoga project which is located at:
https://github.com/Microsoft-CISL/shimoga

To run the source code, the easiest way might be adding the package to your Shimoga project.

Refered Andrew Ng's lecture on Coursera:
https://class.coursera.org/ml-005/lecture

The dataset is from UCI repository (E-mail spam dataset, 39.4% are Spam)
https://archive.ics.uci.edu/ml/datasets/Spambase

The precision of the classification was between 0.43~0.67 which is disastrous.
Possible reasons are:
	1. My implementation is wrong (Probably, this is the case)
	2. The dataset is too complex to be classified by linear classifier
	3. Wrong parameter (I tried many of them though...)
You can check this measure by monitoring the STDOUT of the controller node

This code was tested only on local environment. But, I'm sure the logic is scalable on # of data (May be not on # of dimensions) since it is simple batch processing