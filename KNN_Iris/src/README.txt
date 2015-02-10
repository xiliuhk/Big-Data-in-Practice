IRIS CLASSIFICATION BY MAPREDUCE

Author: LIU Xi, Lacey (AndrewID: xiliu1)
Course: Big Data Systems in Practice
Org: Language Technologies Institute, Carnegie Mellon University, USA

This mapreduce program classifies the Iris based on 4 features. 

0. Directories
input directory: training set, test set, golden standard
output director: result

1. Input Args: input directory | output directory | k_value

2. Output Format: instance data | classification| correctness

3. Similarity Calculation
- Euclidean Similarity
- Cosine Similarity

4. Experiment

#Misclassification
		Euclidean	Cosine
k = 3		23			1			
k = 5		23			1
k = 7		23			1

Conclusion
Cosine similarity outperforms Euclidean similarity. 

Discussion
Possible reason for same classification result for different k: 
a. the training dataset is very small. 
b. the training dataset is very sparse and well clustered.



