package com.harkosoft;

import java.io.Serializable;

public class NameYearCountPojo implements Serializable {
	public NameYearCountPojo(String gender, int year, int rank, String firstName, int count) {
		this.gender = gender.toLowerCase();
		this.year = year;
		this.rank = rank;
		this.firstName = firstName;
		this.count = count;
	}

	public final String gender;
	public final int year;
	public final int rank;
	public final String firstName;
	public final int count;

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{");
		builder.append("\"gender\":\"" + gender + "\"");
		builder.append(",");
		builder.append("\"year\":\"" + year + "\"");
		builder.append(",");
		builder.append("\"rank\":\"" + rank + "\"");
		builder.append(",");
		builder.append("\"firstName\":\"" + firstName + "\"");
		builder.append(",");
		builder.append("\"count\":\"" + count + "\"");
		builder.append("}");
		return builder.toString();
	}

	public String getGenderNameKey() {
		return String.format("%s (%s)", firstName, gender);
	}
}
