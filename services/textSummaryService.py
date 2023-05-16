import pandas as pd

from pysummarization.nlpbase.auto_abstractor import AutoAbstractor
from pysummarization.tokenizabledoc.simple_tokenizer import SimpleTokenizer
from pysummarization.abstractabledoc.top_n_rank_abstractor import TopNRankAbstractor


class textCompressor:
    def __init__(self):
        print("Stage: TextCompressor init:")
              
        
        
        print("End TextCompressor init")
    def summarize(dataFrameIn):
    
        #read in datframe
        data_import1 = dataFrameIn           
        dataFrameSum = pd.DataFrame(columns=["summary"])
    
    
    
    
        for row in data_import1.loc[: , "transcript"]:
        #document = data_import1.loc[: , "transcript"]
        
            print(row) 
            # Object of automatic summarization.
            auto_abstractor = AutoAbstractor()
            # Set tokenizer.
            auto_abstractor.tokenizable_doc = SimpleTokenizer()
            # Set delimiter for making a list of sentence.
            auto_abstractor.delimiter_list = [".", "\n"]
            # Object of abstracting and filtering document.
            abstractable_doc = TopNRankAbstractor()
            # Summarize document.
            result_dict = auto_abstractor.summarize(row, abstractable_doc)
            sentenceList = ""
        # Output result.
            for sentence in result_dict["summarize_result"]:
            
                sentenceList = sentenceList + sentence

                print(sentence)
            
            new_row = pd.Series({'summary': sentenceList })
            dataFrameSum = pd.concat([dataFrameSum, new_row.to_frame().T], ignore_index=True)
    
        new_df = data_import1.join(dataFrameSum) # printing the resultant dataframe print(new_df)
        
        new_df.to_csv("./csv/URLTEMP2.csv")
    
        return new_df


    def keyWords(dataFrameIn):
    
        #read in datframe
        data_import1 = dataFrameIn           
        dataFrameSum = pd.DataFrame(columns=["keywords"])
    #NEED KEYWORDS FUNCTION HERE
