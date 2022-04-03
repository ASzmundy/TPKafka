package Objects;

import java.sql.Timestamp;

public class Country {
    private String country;
    private String countrycode;
    private String slug;
    private int newconfirmed;
    private int totalconfirmed;
    private int newdeaths;
    private int totaldeaths;
    private int newrecovered;
    private int totalrecovered;
    private String datemaj;

    public Country(){

    }

    public Country(String country, String countrycode, String slug, int newconfirmed, int totalconfirmed, int newdeaths, int totaldeaths, int newrecovered, int totalrecovered, String datemaj) {
        this.country = country;
        this.countrycode = countrycode;
        this.slug = slug;
        this.newconfirmed = newconfirmed;
        this.totalconfirmed = totalconfirmed;
        this.newdeaths = newdeaths;
        this.totaldeaths = totaldeaths;
        this.newrecovered = newrecovered;
        this.totalrecovered = totalrecovered;
        this.datemaj = datemaj;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCountrycode() {
        return countrycode;
    }

    public void setCountrycode(String countrycode) {
        this.countrycode = countrycode;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public int getNewconfirmed() {
        return newconfirmed;
    }

    public void setNewconfirmed(int newconfirmed) {
        this.newconfirmed = newconfirmed;
    }

    public int getTotalconfirmed() {
        return totalconfirmed;
    }

    public void setTotalconfirmed(int totalconfirmed) {
        this.totalconfirmed = totalconfirmed;
    }

    public int getNewdeaths() {
        return newdeaths;
    }

    public void setNewdeaths(int newdeaths) {
        this.newdeaths = newdeaths;
    }

    public int getTotaldeaths() {
        return totaldeaths;
    }

    public void setTotaldeaths(int totaldeaths) {
        this.totaldeaths = totaldeaths;
    }

    public int getNewrecovered() {
        return newrecovered;
    }

    public void setNewrecovered(int newrecovered) {
        this.newrecovered = newrecovered;
    }

    public int getTotalrecovered() {
        return totalrecovered;
    }

    public void setTotalrecovered(int totalrecovered) {
        this.totalrecovered = totalrecovered;
    }

    public String getDatemaj() {
        return datemaj;
    }

    public void setDatemaj(String datemaj) {
        this.datemaj = datemaj;
    }
}
