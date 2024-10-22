package com.sentimentanalysis.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class UserActivityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<String, Integer> userActivityCount = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int activityCount = 0;

        for (IntWritable val : values) {
            activityCount += val.get();
        }

        userActivityCount.put(key.toString(), activityCount);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        PriorityQueue<Map.Entry<String, Integer>> topUsersQueue = new PriorityQueue<>((a, b) -> b.getValue().compareTo(a.getValue()));

        topUsersQueue.addAll(userActivityCount.entrySet());

        int counter = 0;
        while (!topUsersQueue.isEmpty() && counter < 10) {
            Map.Entry<String, Integer> entry = topUsersQueue.poll();
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            counter++;
        }
    }
}
