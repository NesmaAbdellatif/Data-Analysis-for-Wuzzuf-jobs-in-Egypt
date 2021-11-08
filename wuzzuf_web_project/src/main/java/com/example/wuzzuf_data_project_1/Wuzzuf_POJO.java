package com.example.wuzzuf_data_project_1;

import java.util.Arrays;

public class Wuzzuf_POJO {
	private String title; 
	private String company; 
	private String location; 
	private String type; 
	private String level; 
	private String years_exp; 
	private String country; 
	private String[] skills;
	
	
	public Wuzzuf_POJO(String title, String company, String location, String type, String level, String years_exp, String country,
                       String[] skills) {
		super();
		this.title = title;
		this.company = company;
		this.location = location;
		this.type = type;
		this.years_exp = years_exp;
		this.country = country;
		this.skills = skills;
	}


	public String getTitle() {
		return title;
	}


	public void setTitle(String title) {
		this.title = title;
	}


	public String getCompany() {
		return company;
	}


	public void setCompany(String company) {
		this.company = company;
	}


	public String getLocation() {
		return location;
	}


	public void setLocation(String location) {
		this.location = location;
	}


	public String getType() {
		return type;
	}


	public void setType(String type) {
		this.type = type;
	}


	public String getYears_exp() {
		return years_exp;
	}


	public void setYears_exp(String years_exp) {
		this.years_exp = years_exp;
	}


	public String getCountry() {
		return country;
	}


	public void setCountry(String country) {
		this.country = country;
	}


	public String[] getSkills() {
		return skills;
	}


	public void setSkills(String[] skills) {
		this.skills = skills;
	}


	@Override
	public String toString() {
		return "[title=" + title + ", company=" + company + ", location=" + location + ", type=" + type
				+ ", years_exp=" + years_exp + ", country=" + country + ", skills=" + Arrays.toString(skills) + "]";
	}


	public String getLevel() {
		return level;
	}


	public void setLevel(String level) {
		this.level = level;
	} 
	
	


}
