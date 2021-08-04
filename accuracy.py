from sys import argv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import math

filename, inputfile = argv

with open(inputfile) as f:
    lines = f.readlines()
    x = [float(line[1:-2].split(',')[0]) for line in lines]
    y = [float(line[1:-2].split(',')[1]) for line in lines]
    labels = ['normal' if line[1:-2].split(',')[2] == 'normal' else 'attack' for line in lines]

logx = [math.log(float(n)) for n in x]
logy = [math.log(float(n)) for n in y]

df = pd.DataFrame(dict(x=logx, y=logy, label=labels))

sample_size = len(x)

# Partitioning the vertices and extracting the medians
normals = df[df['label']=='normal']
sorted_df = normals.sort_values(by=['x'])
normal_count = len(normals)
medians_x = []
medians_y = []
partitions = 30
slice_size = normal_count / partitions
for i in range(partitions):
    start = int(normal_count*(i/partitions))
    end = int(normal_count*((i+1)/partitions))
    sliced = sorted_df.iloc[start:end].sort_values(by=['y'])
    medians_x.append(sliced.iloc[int(slice_size/2)].x)
    medians_y.append(sliced.iloc[int(slice_size/2)].y)
median_df = pd.DataFrame(dict(x=medians_x, y=medians_y, label='median'))
total_df = pd.concat([df, median_df])


#scatter plot
fig, ax = plt.subplots()
colors = {'normal':'aquamarine', 'attack':'crimson', 'median':'dodgerblue'}
marker = {'normal':'.', 'median':'.', 'attack':'2'}
markersize = {'normal':5, 'median':1, 'attack':7}
graph_title = {'feature_comparisons/PvE.txt':'Eigenvector Centrality vs. Edge Counts',
                'feature_comparisons/WvE.txt':'Edge Weights vs. Edge Counts',
                'feature_comparisons/PvW.txt':'Eigenvector Centrality vs. Edge Weights'}
xlabel = {'feature_comparisons/PvE.txt':'Edge Counts',
                'feature_comparisons/WvE.txt':'Edge Counts',
                'feature_comparisons/PvW.txt':'Edge Weights'}
ylabel = {'feature_comparisons/PvE.txt':'Eigenvector Centrality',
                'feature_comparisons/WvE.txt':'Edge Weights',
                'feature_comparisons/PvW.txt':'Eigenvector Centrality'}

label_groups = total_df.groupby('label')
for name, group in label_groups:
    ax.plot(group.x, group.y, marker=marker[name], markersize=markersize[name], linestyle='', label=name, color=colors[name])

ax.set_title(graph_title[inputfile])
ax.set_xlabel(xlabel[inputfile])
ax.set_ylabel(ylabel[inputfile])
ax.legend()


# linear regression line from medians
npmedians = np.array(medians_x)
m, b = np.polyfit(medians_x, medians_y, 1)
ax.plot(medians_x, medians_y, '.', markersize=4, c='dodgerblue')
ax.plot(npmedians, m * npmedians + b, c='dodgerblue')


# calculating the distance of each point to the linreg line
df_x = df.loc[:,'x']
df_y = df.loc[:,'y']
df_label = df.loc[:,'label']

def getDistance(x, y):
    return abs((m*x - y + b) / math.sqrt(m*m + 1))
distance = getDistance(df_x, df_y)

df_dist = pd.concat([distance, df_label], axis=1)
df_dist.columns = ['distance', 'type']


# classification of types
df_truths = df_dist.sort_values(by=['distance'])
classified_normal = df_truths.iloc[:normal_count]
classified_attack = df_truths.iloc[normal_count:]
correct_normal = classified_normal[classified_normal['type']=='normal']
correct_attack = classified_attack[classified_attack['type']=='attack']

# accuracy of detection
accuracy = (len(correct_normal) + len(correct_attack)) / len(df_truths)
print("Number of samples: " + str(sample_size))
print("Number of normals: " + str(normal_count))
print("Number of attacks: " + str(sample_size - normal_count))
print("Anomaly detection accuracy: " + str(accuracy))


plt.show()
