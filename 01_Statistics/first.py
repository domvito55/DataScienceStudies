import numpy as np
import scipy.stats as stats

salaries = np.array([
    40000, 40000, 40000, 40000, 40000, 50000, 50000, 50000, 50000, 50000,
    60000, 60000, 60000, 60000, 60000, 80000, 80000, 80000, 80000, 80000,
    70000, 70000, 70000, 70000, 70000, 90000, 90000, 90000, 90000, 90000,
    90000, 90000, 90000, 90000, 90000, 160000, 160000, 160000, 160000, 160000,
    100000, 100000, 110000, 110000, 120000, 120000, 130000, 130000, 140000, 140000,
    150000, 170000, 180000, 180000, 230000
])

# mean, median, mode
print("mean: ", salaries.mean())
print("median: ", np.median(salaries))
print("mode: ", stats.mode(salaries))

#percentile
print("90th percentile: ", np.percentile(salaries, 90))

#boxplot
import matplotlib.pyplot as plt

plt.boxplot(salaries, patch_artist=True, notch='True', vert=False)
plt.show()

salaries[salaries<60000]

s2 = salaries.reshape(-1,5)
s2
s2[s2<60000]

import pandas as pd

a = pd.Series([1,2])

a.shape

b = pd.read_csv('test.csv')

b.loc[0]

b[b>100]

c = pd.DataFrame(np.array([[1,2,3],[4,5,6]]))

d = pd.DataFrame([1])

f = salaries.reshape((-1,1))
salaries.shape


# --------------------
test = np.array([72, 94, 67, 90, 88, 90, 82, 92, 75])
print("mean: ", test.mean())


s2

df = pd.DataFrame(s2)
df
df2 = df.drop(range(7))
df2.index = ['a', 'b', 'c', 'd']
df2.columns = ['e', 'f', 'g', 'h', 'i']
df2
df2.rename({'a': 'ramiro'})

df
pd.concat([df, df2], axis=1)

df
df.mean()

b
b.groupby('index').mean()

s2

days = range(1,8)
temp = np.random.randint(20,30,7)
temp2 =  np.random.randint(20,30,7)

df3 = pd.DataFrame({'days': days, 'temp': temp})
df3

df4 = pd.DataFrame({'days': days, 'temp': temp2})
df4

from matplotlib import pyplot as plt

# ------------ line plot
plt.plot(df3['days'], df3['temp'])
plt.xlabel('Days')
plt.ylabel('temperature [C]')
plt.title('Example of line plot')
plt.show()

plt.plot(df3['days'], df3['temp'], label='temp1')
plt.plot(df4['days'], df4['temp'], label='temp2')
plt.xlabel('Days')
plt.ylabel('temperature [C]')
plt.title('Example of 2 lines in the same plot')
# plt.legend()
plt.show()

# ------------ scatter plot
plt.scatter(df3['days'], df3['temp'], label='temp1')
plt.scatter(df4['days'], df4['temp'], label='temp2')
plt.xlabel('Days')
plt.ylabel('temperature [C]')
plt.title('Example of 2 scatters in the same plot')
plt.legend()
plt.show()

# ------------ bar plot
plt.bar(df3['days'], df3['temp'], label='temp1')
plt.bar(df4['days'], df4['temp'], label='temp2', alpha=0.5, width=0.5)
plt.xlabel('Days')
plt.ylabel('temperature [C]')
plt.title('Example of 2 bar plots in the same plot')
plt.legend()
plt.show()

# ------------ bar plot

plt.pie(df2['e'], labels=df2.index, autopct='%1.1f%%')
plt.title('Example of pichart')
plt.show()

# ------------ hist plot
plt.hist(salaries, rwidth=0.9)
plt.xlabel('Salaries')
plt.ylabel('Frequency')
plt.title('Example of histogram')
plt.legend()
plt.show()

# ------------ subplots
figure, subplots = plt.subplots(1, 2, figsize=(8,4))
subplots[0].plot(df3['days'], df3['temp'])
subplots[0].set_title('Line1') 
subplots[1].plot(df4['days'], df4['temp'], color='green')
subplots[1].set_title('Line2')

for subplot in subplots:
    subplot.set(xlabel='day', ylabel='temperatur [C]')

plt.show()


import seaborn as sns

# ------------- seaborn boxplot
sns.boxplot(x='days',y='temp',data=df3, notch=True)
plt.show()

# ------------- density plot
normal = np.random.normal(0, 1, 200)
sns.kdeplot(normal)
plt.show()

normal = np.random.normal(0, 1, 200)
normal2 = np.random.normal(0, 1, 200)
normal3 = normal*2 - 20 + normal2

df5 = pd.DataFrame({'col1': normal, 'col2': normal2})
df5.head()
sns.kdeplot(x=normal, y=normal3)
plt.show()

# ------------- density plot
normal = np.random.normal(0, 1, 200)
sns.violinplot(normal)
plt.show()
