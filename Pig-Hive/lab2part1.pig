wh_data = load 'comqa_train.txt' using PigStorage('\n') as (text:chararray);

questions = filter wh_data by (text matches '(.*(who|what|where|when|how|why|how|whom|which|whose)+.*)+.*\\?.*');

words = foreach questions generate flatten(TOKENIZE(text)) as word;

all_wh_words = filter words by word matches '(who|what|where|when|how|why|how|whom|which|whose).*';

words_without_extras = foreach all_wh_words generate REPLACE(word ,'(\\?|\'s)','') as word;

pure_wh_words = filter words_without_extras by word matches '(who|what|where|when|how|why|how|whom|which|whose)';

wh_words = group pure_wh_words by word;

finalout = foreach wh_words generate group, COUNT(pure_wh_words);

dump finalout;