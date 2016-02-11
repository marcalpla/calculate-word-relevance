# Calculate the relevance of each word from a list of phrases

Spark application wrote in Scala to calculate the relevance of each word from a list of phrases using a initial weight value for each phrase.

The application expects a input CSV file with 2 columns from a Amazon S3 path:

- Phrase or set of strings
- Weight or relevance (expressed with a integer value) to give to the words of the phrase

Generates a list of unique words with the weight or relevance value associated (ordered from more relevance to less) based on the times that the word appears over the entire list of phrases and the initial weight given to each phrase. The output is wrote in a CSV file with 2 columns to a Amazon S3 path:

- Word
- Weight or relevance
