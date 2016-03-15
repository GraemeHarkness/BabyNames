package com.harkosoft;

import java.io.PrintStream;
import java.util.Set;
import java.util.TreeSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.ImmutableSet;

public class NameStatsApp {
	private static final int THRESHOLD = 5;
	private static final int CENTURY = 100;

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NameStatsApp").setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = ctx.textFile("all.csv");
		JavaRDD<NameYearCountPojo> name_year_count = lines.map(csvToPojo()).filter(record -> record.count > THRESHOLD);
		JavaRDD<Iterable<NameYearCountPojo>> popularRecords = name_year_count //
				.mapToPair(keyByGenderAndName()) //
				.groupByKey() //
				.map(pair -> pair._2);

		JavaRDD<String> usageOverTime = popularRecords.map(findUsedUnusedTransitionsOverTime());
		List<String> perpetuals = usageOverTime.filter(line -> line.contains("|Y|")).collect();
		List<String> gones = usageOverTime.filter(line -> line.contains("|Y.|")).collect();
		List<String> news = usageOverTime.filter(line -> line.contains("|.Y|")).collect();

		System.out.println("PERPETUALS: " + perpetuals.size());
		print(perpetuals, System.out);

		System.out.println("GONES: " + gones.size());
		print(gones, System.out);

		System.out.println("NEWS: " + news.size());
		print(news, System.out);

		ctx.close();
	}

	private static enum State {
		START, NAME_USED, NAME_UNUSED, END;

		public String toString() {
			switch (this) {
			case START:
			case END:
				return "|";
			case NAME_USED:
				return "Y";
			case NAME_UNUSED:
				return ".";
			default:
				return null;
			}
		}
	}

	private static Set<Integer> makeSetOfYearsInWhichNameWasUsed(Iterable<NameYearCountPojo> iRecords) {
		Set<Integer> years = new TreeSet<>();
		for (NameYearCountPojo record : iRecords) {
			years.add(record.year);
		}
		return ImmutableSet.copyOf(years);
	}

	private static Function<Iterable<NameYearCountPojo>, String> findUsedUnusedTransitionsOverTime() {
		return iRecords -> {
			Set<Integer> yearsUsed = makeSetOfYearsInWhichNameWasUsed(iRecords);
			StringBuilder statusBuilder = new StringBuilder();
			StringBuilder yearBuilder = new StringBuilder();

            NameYearCountPojo first = iRecords.iterator().next();

			statusBuilder.append( String.format( "%-40s: ", first.getGenderNameKey() ));

			State currentState = State.START;
			statusBuilder.append(currentState.toString());
			for (int year = 1974; year < 2015; ++year) {
				final State thisYearsState = yearsUsed.contains(year) ? State.NAME_USED : State.NAME_UNUSED;
				yearBuilder.append((thisYearsState == State.NAME_USED) ? String.format("%02d", year % CENTURY) : "--");
				yearBuilder.append(" ");
				if (thisYearsState != currentState) {
					currentState = thisYearsState;
					statusBuilder.append(currentState.toString());
				}
			}
			currentState = State.END;
			statusBuilder.append(currentState.toString());
			return statusBuilder.toString() + " " + yearBuilder.toString();
		};
	}

	private static PairFunction<NameYearCountPojo, String, NameYearCountPojo> keyByGenderAndName() {
		return data -> new Tuple2<String, NameYearCountPojo>(data.getGenderNameKey(), data);
	}

	private static Function<String, NameYearCountPojo> csvToPojo() {
		return line -> {
			final String[] split = line.split(",");
			final String gender = split[0];
			final int year = new Integer(split[1]);
			final int rank = new Integer(split[2]);
			final String firstName = split[3];
			final int count = new Integer(split[4]);
			NameYearCountPojo node = new NameYearCountPojo(gender, year, rank, firstName, count);
			return node;
		};
	}

	private static void print(List<String> blob, PrintStream out) {
		for (String output : blob) {
			out.println(output);
		}
	}
}
