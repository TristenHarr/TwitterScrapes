import numpy as np
import matplotlib.pyplot as plt

def counter(item):
    my_dict = {}
    for i in range(item.count()['plain_text']):
        items = item.iloc[i]['plain_text'].split(' ')
        for item2 in items:
            if item2.capitalize() in my_dict.keys():
                my_dict[item2.capitalize()] += 1
            else:
                my_dict[item2.capitalize()] = 1
    return my_dict

def top_words(item, word_count=10, strip_words=False):
    my_dict = counter(item)
    strips = ['The', 'A', 'To','In','On','Of','For','More','I','You', "This", "She","He","Will","Has",
              'Had', "From", "Out","Our","By","Is","No"]
    my_list = []
    if strip_words:
        for item in my_dict.keys():
            if item not in strips:
                my_list.append((my_dict[item], item))
    else:
        for item in my_dict.keys():
            my_list.append((my_dict[item], item))
    plot = sorted(my_list)[::-1]
    objects = list(map(lambda x:x[1], plot))[0:word_count]
    y_pos = np.arange(len(objects))
    performance = list(map(lambda x:x[0], plot))[0:word_count]
    plt.bar(y_pos, performance, align='center')
    plt.xticks(y_pos, objects)
    plt.ylabel('Occurences')
    plt.title('Word Frequency')
    plt.show()