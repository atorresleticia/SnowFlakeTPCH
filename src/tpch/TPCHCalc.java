package tpch;

public class TPCHCalc {

    private double queryTotalTime;
    private double refreshTotalTime;
    private double powerQph;
    private double throughputQph;
    private double QphH;

    public TPCHCalc() {
        this.queryTotalTime = 1;
        this.refreshTotalTime = 1;
    }

    public void setSumQueryTotalTime(double[] queryTimes) {

        this.queryTotalTime = 0;

        for (double time :
                queryTimes) {
            this.queryTotalTime += time;
        }
    }

    public void setSumRefreshTotalTime(double[] refreshTimes){

        this.refreshTotalTime = 0;

        for (double time :
                refreshTimes) {
            this.refreshTotalTime += time;
        }
    }

    public void setQueryTotalTime(double[] queryTimes) {

        this.queryTotalTime = 1;

        for (double time :
                queryTimes) {
            this.queryTotalTime *= time;
        }
    }

    public void setRefreshTotalTime(double[] refreshTimes){

        this.refreshTotalTime = 1;

        for (double time :
                refreshTimes) {
            this.refreshTotalTime *= time;
        }
    }

    public double getQueryTotalTime() {
        return queryTotalTime;
    }

    public double getRefreshTotalTime() {
        return refreshTotalTime;
    }

    public void setPowerQph(int scaleFactor) {

        double timesFactor = 3600 * scaleFactor;
        double queryTimesRefresh = this.queryTotalTime * this.refreshTotalTime;
        double root = Math.pow(queryTimesRefresh, (1 / 15));

        this.powerQph = timesFactor / root;
    }

    public void setThroughputQph(int scaleFactor, int streams, double time) {

        double timesStream = streams * 3600 * 13;
        double timesFactor = time * scaleFactor;

        this.throughputQph = timesStream / timesFactor;
    }

    public void setQphH() {
        this.QphH = Math.sqrt(this.powerQph * this.throughputQph);
    }

    public double getPowerQph() {
        return powerQph;
    }

    public double getThroughputQph() {
        return throughputQph;
    }

    public double getQphH() {
        return QphH;
    }
}
