package tpch;

public class TPCHCalc {

    private double queryTotalTime;
    private double refreshTotalTime;
    private long powerQph;
    private long throughputQph;
    private long QphH;

    public TPCHCalc() {
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

        long timesFactor = 3600 * scaleFactor;
        long queryTimesRefresh = (long) (this.queryTotalTime * this.refreshTotalTime);

        System.out.println(scaleFactor + "GB: q: " + this.queryTotalTime + " rf: " + this.refreshTotalTime + "total: " + queryTimesRefresh);

        long root = (long) Math.pow(queryTimesRefresh, (1 / 17));

        System.out.println("R: " + root);

        this.powerQph = timesFactor / root;
    }

    public void setThroughputQph(int scaleFactor, int streams, double time) {

        long timesStream = streams * 3600 * 15;
        long timesFactor = (long) (time * scaleFactor);

        this.throughputQph = timesStream / timesFactor;
    }

    public void setQphH() {
        this.QphH = (long) Math.sqrt(this.powerQph * this.throughputQph);
    }

    public long getPowerQph() {
        return powerQph;
    }

    public long getThroughputQph() {
        return throughputQph;
    }

    public long getQphH() {
        return QphH;
    }
}
