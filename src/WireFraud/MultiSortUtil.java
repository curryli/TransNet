package WireFraud;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;


public class MultiSortUtil {
	
	// 自己定义的key类应该实现WritableComparable接口
	public static class KeyPair implements WritableComparable<KeyPair> {
		String first;  //第一排序字段
		String second; //第二排序字段

		public KeyPair() {}

		//Set the left and right values.
		public void set(String left, String right) {
			first = left;
			second = right;
		}

		public String getFirst() {
			return first;
		}

		public String getSecond() {
			return second;
		}

		/**
		 * 反序列化，从流中的二进制转换成KeyPair
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readUTF();
			second = in.readUTF();
		}

		/**
		 * 序列化，将KeyPair转化成使用流传送的二进制
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(first);
			out.writeUTF(second);
		}

		/**
		 * key的比较
		 */
		@Override
		public int compareTo(KeyPair o) {
			if (!first.equals(o.first)) {
				return first.compareTo(o.first);
			} else {
				// 由大到小排序
				return o.second.compareTo(second);
			}
		}

		// 新定义类应该重写的两个方法
		@Override
		// The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
		public int hashCode() {
			return first.hashCode() + second.hashCode();
		}

		@Override
		public boolean equals(Object right) {
			if (right == null)
				return false;
			if (this == right)
				return true;
			if (right instanceof KeyPair) {
				KeyPair kp = (KeyPair) right;
				return kp.first.equals(first) && kp.second.equals(second);
			} else {
				return false;
			}
		}
	}

	/**
	 * 分区函数类。根据first确定Partition。
	 */
	public static class FirstPartitioner extends Partitioner<KeyPair, Text> {
		@Override
		public int getPartition(KeyPair key, Text value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode()) % numPartitions;
		}
	}

	/**
	 * 分组函数类。只要first相同就属于同一个组。
	 */
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(KeyPair.class, true);
		}

		// Compare two WritableComparables.
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			KeyPair kp1 = (KeyPair) w1;
			KeyPair kp2 = (KeyPair) w2;
			String kp1First = kp1.getFirst();
			String kp2First = kp2.getFirst();
			return kp1First.compareTo(kp2First);
		}
	}

}
