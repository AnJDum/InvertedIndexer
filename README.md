# 大数据作业六

191098273 徐冰清

## 作业要求

在作业5的数据集基础上完成莎士比亚文集单词的倒排索引，输出按字典序对单词进行排序，单词的索引按照单词在该文档中出现的次数从大到小排序。单词忽略大小写，忽略标点符号（punctuation.txt），忽略停词（stop-word-list.txt），忽略数字，单词长度>=3。输出格式如下：

单词1: 文档i#频次a, 文档j#频次b...

单词2: 文档m#频次x, 文档n#频次y...

...

## 思路分析

根据老师分享在群里的倒排索引代码`InvertedIndexer.java`，加以适当改写，目的有三：

一是对单词做出处理

二是在输出时按照单词在该文档中出现的次数从大到小排序

三是使输出满足一定的格式规范

### 整体架构

#### Map

##### 1. InvertedIndexMapper

| 输入 | key: 文件当前行偏移位置，value: 文件当前行内容 |
| ---- | ---------------------------------------------- |
| 输出 | key: word#filename，value: 1                   |

使用默认的 TextInputFormat 类对输入文件进行处理，得到文本中每行的偏移量及其内容。获取当前处理文件名filename，对value值进行切分得到多个word值，将每个word与filename拼接到一起作为输出key，其计数值为1，即value为1。

##### 2. SumCombiner

| 输入 | key: word#filename，value: [1, 1, 1, …]      |
| ---- | -------------------------------------------- |
| 输出 | key: word#filename，value: 同一key下的累加和 |

将Mapper输出的中间结果相同key部分的value累加，经过 map 方法处理后， Combine 过程将 key 值相同的 value 值累加，得到一个单词在文档中的词频。

#### Reduce

##### 1. NewPartitioner

| 输入 | key: word#filename，value: 累加和 |
| ---- | --------------------------------- |
| 输出 | key: word#filename，value: 累加和 |

##### 2. InvertedIndexReducer

| 输入 | key: word#filename，vaule: [累加和1, 累加和2, …] |
| ---- | ------------------------------------------------ |
| 输出 | key: word,filename:词频;filename:词频;…          |

利用Reduce节点输入的key值都是有序的，将key拆分，对于同一word，每次都保存其filename和词频，并统计其总出现次数和总出现文档数；当同一word处理完后，filename及其词频作为value输出。



### 单词处理

将所有字母转为小写

```java
String line = value.toString().toLowerCase();
```

忽略标点符号

```java
Path punctuationsPath = new Path(patternsURIs[1].getPath());
String punctuationsFileName = punctuationsPath.getName().toString();
parseSkipPunctuations(punctuationsFileName); 

fis = new BufferedReader(new FileReader(fileName));
String pattern = null;
while ((pattern = fis.readLine()) != null) {
  patternsToSkip.add(pattern);
}

for (String pattern : punctuations) {
        line = line.replaceAll(pattern, " ");
}
```

忽略长度小于3的单词

```java
StringTokenizer itr = new StringTokenizer(line);
while (itr.hasMoreTokens()) {
	String one_word = itr.nextToken();
	if(one_word.length()<3) {
		continue;
	}
}
```

忽略停词文件里的词

```java
Path patternsPath = new Path(patternsURIs[0].getPath());
String patternsFileName = patternsPath.getName().toString();
parseSkipFile(patternsFileName);

fis = new BufferedReader(new FileReader(fileName));
String pattern = null;
while ((pattern = fis.readLine()) != null) {
  patternsToSkip.add(pattern);
}

if(patternsToSkip.contains(one_word)){
	continue;
}
```

### 次数排序

对`InvertedIndexReducer`中的`reduce`函数进行更改。因为默认对文件名排序，所以在设置word2的时候，将数字放在前面。

```java
word2.set("<" + sum + "#" + temp + ">");
postingList.sort(Collections.reverseOrder());
```

### 输出格式

首先以数字+文件名的形式存入，再通过substring取出正确的顺序。

```java
word2.set("<" + sum + "#" + temp + ">");
String currentItem = CurrentItem + ":";
Text myItem = new Text(currentItem);
for (String p : postingList) {
  String q = p.substring(p.indexOf("#")+1, p.indexOf(">")) + "#" + p.substring(p.indexOf("<")+1, p.indexOf("#"));
  out.append(q);
  out.append(",");
   count =
          count
              + Long.parseLong(p.substring(p.indexOf("<") + 1,
                   p.indexOf("#")));
}
```



## 运行截图

### 伪分布与集群

![截屏2021-11-03 下午7.15.24](/Users/anjdum/Documents/大学/金融大数据处理技术/作业/作业6/WordCount/img/截屏2021-11-03 下午7.15.24.png)

![截屏2021-11-03 下午7.15.16](/Users/anjdum/Documents/大学/金融大数据处理技术/作业/作业6/WordCount/img/截屏2021-11-03 下午7.15.16.png)

![截屏2021-11-03 下午6.04.50](/Users/anjdum/Documents/大学/金融大数据处理技术/作业/作业6/WordCount/img/截屏2021-11-03 下午6.04.50.png)

### web

![截屏2021-11-03 下午7.15.46](/Users/anjdum/Documents/大学/金融大数据处理技术/作业/作业6/WordCount/img/截屏2021-11-03 下午7.15.46.png)

![截屏2021-11-03 下午7.15.40](/Users/anjdum/Documents/大学/金融大数据处理技术/作业/作业6/WordCount/img/截屏2021-11-03 下午7.15.40.png)
