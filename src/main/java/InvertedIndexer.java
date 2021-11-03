import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndexer {
  /** 自定义FileInputFormat **/
  public static class FileNameInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      FileNameRecordReader fnrr = new FileNameRecordReader();
        fnrr.initialize(split, context);
      return fnrr;
    }
  }

  /** 自定义RecordReader **/
  public static class FileNameRecordReader extends RecordReader<Text, Text> {
    String fileName;
    LineRecordReader lrr = new LineRecordReader();

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return new Text(fileName);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return lrr.getCurrentValue();
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
        throws IOException, InterruptedException {
      lrr.initialize(arg0, arg1);
      fileName = ((FileSplit) arg0).getPath().getName();
    }

    public void close() throws IOException {
        lrr.close();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
      return lrr.nextKeyValue();
    }

    public float getProgress() throws IOException, InterruptedException {
      return lrr.getProgress();
    }
  }


  public static class InvertedIndexMapper extends
      Mapper<Text, Text, Text, IntWritable> {
    static enum CountersEnum { INPUT_WORDS }
    private Set<String> patternsToSkip = new HashSet<String>();
    private Set<String> punctuations = new HashSet<String>();
    private Configuration conf;
    private BufferedReader fis;
    private boolean caseSensitive;
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    public void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true); // 配置文件中的wordcount.case.sensitive功能是否打开

      URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); // getCacheFiles()方法可以取出缓存的本地化文件，本例中在main设置

      Path patternsPath = new Path(patternsURIs[0].getPath());
      String patternsFileName = patternsPath.getName().toString();
      parseSkipFile(patternsFileName); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String fileName)

        Path punctuationsPath = new Path(patternsURIs[1].getPath());
        String punctuationsFileName = punctuationsPath.getName().toString();
        parseSkipPunctuations(punctuationsFileName); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String fileName)

    }


    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));

        String pattern = null;
        while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file "
                + StringUtils.stringifyException(ioe));
      }
    }

    private void parseSkipPunctuations(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));

        String pattern = null;
        while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
          punctuations.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file "
                + StringUtils.stringifyException(ioe));
      }
    //  System.out.println(punctuations);
    }

    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString().toLowerCase();
      for (String pattern : punctuations) {
        line = line.replaceAll(pattern, " ");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String one_word = itr.nextToken();
        if(one_word.length()<3) {
          continue;
        }
        if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(one_word).matches()) {
          continue;
        }
        if(patternsToSkip.contains(one_word)){
          continue;
        }
        word.set(one_word);
        word.set(word + "#" + key);
        context.write(word, one);
      }

    }
  }

  /** 使用Combiner将Mapper的输出结果中value部分的词频进行统计 **/
  public static class SumCombiner extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  /** 自定义HashPartitioner，保证 <term, docid>格式的key值按照term分发给Reducer **/
  public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
      String term = new String();
      term = key.toString().split("#")[0]; // <term#docid>=>term
      return super.getPartition(new Text(term), value, numReduceTasks);
    }
  }

  public static class InvertedIndexReducer extends
      Reducer<Text, IntWritable, Text, Text> {
    private Text word1 = new Text();
    private Text word2 = new Text();
    String temp = new String();
    static Text CurrentItem = new Text(" ");
    static List<String> postingList = new ArrayList<String>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      word1.set(key.toString().split("#")[0]);
      temp = key.toString().split("#")[1];
      for (IntWritable val : values) {
        sum += val.get();
      }
      word2.set("<" + sum + "#" + temp + ">");
      if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
        StringBuilder out = new StringBuilder();
        long count = 0;
        String currentItem = CurrentItem + ":";
        Text myItem = new Text(currentItem);
        postingList.sort(Collections.reverseOrder());

        for (String p : postingList) {
          String q = p.substring(p.indexOf("#")+1, p.indexOf(">")) + "#" + p.substring(p.indexOf("<")+1, p.indexOf("#"));
          out.append(q);
          out.append(",");
          count =
              count
                  + Long.parseLong(p.substring(p.indexOf("<") + 1,
                      p.indexOf("#")));
        }
        if (count > 0)
          context.write(myItem, new Text(out.toString()));
        postingList = new ArrayList<String>();
      }
      CurrentItem = new Text(word1);
      postingList.add(word2.toString()); // 不断向postingList也就是文档名称中添加词表
    }

    // cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况

    public void cleanup(Context context) throws IOException,
        InterruptedException {
      StringBuilder out = new StringBuilder();
      long count = 0;
      for (String p : postingList) {
        String q = p.substring(p.indexOf("#")+1, p.indexOf(">")) + "#" + p.substring(p.indexOf("<")+1, p.indexOf("#"));
        out.append(q);
        out.append(",");
        count =
            count
                + Long
                    .parseLong(p.substring(p.indexOf("<") + 1, p.indexOf("#")));
      }
  //    out.append("<total," + count + ">.");
      if (count > 0)
        context.write(CurrentItem, new Text(out.toString()));
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    Job job = new Job(conf, "inverted index");
    List<String> otherArgs = new ArrayList<String>();
    job.addCacheFile(new Path(remainingArgs[2]).toUri());
    job.addCacheFile(new Path(remainingArgs[3]).toUri());


    for (int i = 0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
        job.setJarByClass(InvertedIndexer.class);
        job.setInputFormatClass(FileNameInputFormat.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}