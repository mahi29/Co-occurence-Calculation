import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This is the skeleton for CS61c project 1, Fall 2012.
 *
 * Contact Alan Christopher or Ravi Punj with questions and comments.
 *
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */
  public class Proj1 {
    


    /** An Example Writable which contains two String Objects. */
    public static class StringPair implements Writable {
        /** The String objects I wrap. */
	private String a, b;

	/** Initializes me to contain empty strings. */
	public StringPair() {
	    a = b = "";
	}
	
	/** Initializes me to contain A, B. */
        public StringPair(String a, String b) {
            this.a = a;
	    this.b = b;
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new Text(a).write(out);
	    new Text(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
	    Text tmp = new Text();
	    tmp.readFields(in);
	    a = tmp.toString();
	    
	    tmp.readFields(in);
	    b = tmp.toString();
        }

	/** Returns A. */
	public String getA() {
	    return a;
	}
	/** Returns B. */
	public String getB() {
	    return b;
	}
    }


  /**
   * Inputs a set of (docID, document contents) pairs.
   * Outputs a set of (Text, Text) pairs.
   */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, StringPair> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");

        private String targetGram = null;
	private int funcNum = 0;
	public int n;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
        public void setup(Context context) {
            targetGram = context.getConfiguration().get("targetGram").toLowerCase();
	    targetGram = targetGram.trim();
	    String[] tG = targetGram.split(" ");
	    
	    n = tG.length;   //Number of words in the targetGram;
	    
	    try {
		funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
	    } catch (NumberFormatException e) {
		/* Do nothing. */
	    }
        }

        @Override
        public void map(WritableComparable docID, Text docContents, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
	    Func func = funcFromNum(funcNum);
	    int startIndex = matcher.regionStart();
	    int endIndex = matcher.regionEnd();
	    ArrayList<String> altWords = new ArrayList<String>();
    	    ArrayList<String> words = new ArrayList<String>();  //An array list of all the n-grams in the document. 
	    //HashMap<Text,DoubleWritable>  hT = new HashMap<Text,DoubleWritable>();
	    String gram  = "";

	    while(matcher.find()){
		String blah = matcher.group();
		blah = blah.toLowerCase();
		altWords.add(blah);
	    }

	    for (int i  = 0; i < altWords.size()-n+1;i++){
		String a = "";
		int j;
		for (j = i; j < i+n; j++){
		
		    String b = (String) altWords.get(j);
		    a = a+" "+b;


		    if (j==(i+n-1)){
			a = a.trim();
			//System.out.println("A: " + a);
			words.add(a);
			a = "";
		    }

		}
		if (j == (altWords.size() - 1) && i == (altWords.size() -n)) {
		  String last = (String) altWords.get(j);
		  String fin = a + " " + last;
		  words.add(fin);
		  }

	    }
	    int count = 0;

	    //Gets a count of the number of targetGrams in the document
	    for (int i = 0; i < words.size(); i++) {
		String w = words.get(i);
		if (w.equals(targetGram)){
		    count++;
			}
	    }
	    
	    if (count == 0){
		Text noTG = new Text(targetGram);
		StringPair pI1 = new StringPair("infinity","1");
		//DoubleWritable posInf = new DoubleWritable(Double.POSITIVE_INFINITY);
		context.write(noTG,pI1);
	    }
	    int[] targetLocations = new int[count];
	    
	    //Gets the indices of all the targetGrams in the document
	   int c = 0;
	    for (int i = 0; i< words.size(); i++){
		if(words.get(i).equals(targetGram)) {
		    targetLocations[c] = i;
		    c++;
		}
	    }
	    
	   
	    //Outputs all the n-grams to the reducer 
	    if (count != 0) {
		for(int i = 0; i<words.size();i++){
		    String ngram = words.get(i);
		    double ab = (double) targetLocations[0];
		    double dmin = (double) Math.abs(ab-i);
		    for (int t = 1; t< targetLocations.length; t++){
			double tort  = (double) Math.abs((targetLocations[t]-i));
			if (tort < dmin) {
			    dmin = tort;
			}
		    }
		    DoubleWritable distance = new DoubleWritable(dmin);
		    Text key = new Text(ngram);
		    double huk = distance.get();
		    double idra = func.f(huk);
		    DoubleWritable realDist = new DoubleWritable(idra);
		    if (!(ngram.equals(targetGram))){
			StringPair sPoutput = new StringPair(Double.toString(idra), "1");			
			context.write(key,sPoutput);
		    }
		    // context.write(key,realDist);
		}
	    } else {
		for (int i = 0; i < words.size(); i++){
		    String ngram  = words.get(i);
		    DoubleWritable valp = new DoubleWritable(Double.POSITIVE_INFINITY);
		    if (!(ngram.equals(targetGram))) {
		      Text outp = new Text(ngram);
		      StringPair sP1 = new StringPair("infinity", "1");                //Always (distance, number)
		      context.write(outp,sP1);
		    }
		}
	    }
		

	    /* Iterator it = hT.entrySet().iterator();
	    
	    //Iterates through 'it' and outputs to the reducer a <Text, DoubleWritable> pair. 
	    while (it.hasNext()) {
		Map.Entry kvpair = (Map.Entry)it.next();
		Text key = (Text) kvpair.getKey();
		DoubleWritable value = (DoubleWritable) kvpair.getValue();    //DO FUNC.F OR SOMETHING? THIS IS AN ERROR!!!!
		double huk = value.get();
		double idra = func.f(huk);
		DoubleWritable mma = new DoubleWritable(idra);

		context.write(key,value);
		}*/
		
	    //Maybe read in the input?
	    /*parse:
            while (true) {

            }*/
	    //Maybe do something with the input?

	    //Maybe generate output?
	}	//Closes IOException

	/** Returns the Func corresponding to FUNCNUM*/
	private Func funcFromNum(int funcNum) {
	    Func func = null;
	    switch (funcNum) {
	    case 0:	
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
			}			
		    };	
		break;
	    case 1:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
			}			
		    };
		break;
	    case 2:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : Math.sqrt(d);
			}			
		    };
		break;
	    }
	    return func;
	}
    }

    /** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, StringPair, Text, StringPair> {

      @Override
      public void reduce(Text key, Iterable<StringPair> values,
              Context context) throws IOException, InterruptedException {
	   Iterator val = values.iterator();
	  int sum = 0;
	  double totalD = 0.0;
	  
	  while(val.hasNext()) {
	      StringPair d = (StringPair)val.next();
	      String dist = d.getA();
	      String num = d.getB();
	      double ddist;
	      if (dist.equals("infinity")) {
	           ddist = 0.0;
	       }else {
		    ddist = Double.parseDouble(dist);
	       }
	      totalD = totalD + ddist;
	      sum = sum+ Integer.parseInt(num);
	  }
	  String s1 = Double.toString(totalD);
	  String s2 = Integer.toString(sum);
	  StringPair output1 = new StringPair(s1,s2);//distance, sum
	  context.write(key,output1);
	      
	  
      }
    
    }
	//DO NOT CHANGE IT FROM DOUBLEWRITABLE!! SERIOUSLY, DON'T!
	public static class Reduce1 extends Reducer<Text, StringPair, DoubleWritable, Text> {	//final input s/b DW, not Text
	  @Override
	  public void reduce(Text key, Iterable<StringPair> values,
		    Context context) throws IOException, InterruptedException {
	      Iterator it = values.iterator();
	      Double sum = 0.0;
	      int counter = 0;
	      while(it.hasNext()) {
		  StringPair t =  (StringPair)it.next();
		  String a = t.getA();//distance
		  String b = t.getB();//number
		  Double temp  = 0.0;
		  if (a.equals("infinity")){
		      temp = 0.0;
		  } else {
		      temp  = Double.parseDouble(a);
		  }
		  sum += temp;
		  counter += Integer.parseInt(b);
	      }
	      double temp;
	      if (sum != 0.0) {
		  temp = ((sum * Math.pow((Math.log(sum)),3.0))/counter);
	      } else {
		  temp  = 0.0;
	      }
	      DoubleWritable output1 = new DoubleWritable(temp*-1.0);
	      StringPair hell = new StringPair("this", "sucks");
	      String wtf = Double.toString(temp);
	      Text work = new Text(wtf);
	      context.write(output1,key);	// dW/Text	
	  }
	}

    public static class Map2 extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
	//maybe do something, maybe don't
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {    
      int n = 0;
      static int N_TO_OUTPUT = 100;	//s/b 100
      /*
       * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
       * It's a good place to do configuration or setup that can be shared across many calls to reduce
       */
      @Override
      protected void setup(Context c) {
        n = 0;
      }

	    @Override
	    public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	      Iterator it = values.iterator();
	      ArrayList<Text> aVal = new ArrayList<Text>();
	      int counter = 0;
	      while(it.hasNext()) {
		  double f = key.get();
		  double pos = Math.abs(f);
		  DoubleWritable fKey = new DoubleWritable(pos);
		  Text  fVal = (Text)it.next();
		  if (counter <= N_TO_OUTPUT) {
		    context.write(fKey,fVal);
		    counter++;
		  } else {
		    break;
		    }
	      }

        }
    }
	

    /*
     *  You shouldn't need to modify this function much. If you think you have a good reason to,
     *  you might want to discuss with staff.
     *
     *  The skeleton supports several options.
     *  if you set runJob2 to false, only the first job will run and output will be
     *  in TextFile format, instead of SequenceFile. This is intended as a debugging aid.
     *
     *  If you set combiner to false, neither combiner will run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
	System.out.println("Starting main method");
	GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", false);

        if(runJob2)
          System.out.println("running both jobs");
        else
          System.out.println("for debugging, only running job 1");

        if(combiner)
          System.out.println("using combiner");
        else
          System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
          System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
          System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
          System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
          System.exit(1);
        }
    

        {
            Job firstJob = new Job(conf, "wordcount+co-occur");

            firstJob.setJarByClass(Map1.class);

	    /* You may need to change things here */
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(StringPair.class);
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);	//Changed from DW
	    /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
              firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
              firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "sort");

            secondJob.setJarByClass(Map1.class);
	    /* You may need to change things here */
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            if(combiner)
              secondJob.setCombinerClass(Reduce2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }
}

	




