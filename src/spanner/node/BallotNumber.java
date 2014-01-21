package spanner.node;

public class BallotNumber implements Comparable{

	private int ballotNo;
	private int nodeNo;
	
	public BallotNumber(int ballotNo, int nodeNo)
	{
		this.ballotNo = ballotNo;
		this.nodeNo = nodeNo;
	}
	
	public int getBallotNo()
	{
		return this.ballotNo;
	}
	
	public int getNodeNo()
	{
		return this.nodeNo;
	}
	
	public String toString()
	{
		return "BallotNo : "+this.ballotNo+", NodeNo : "+this.nodeNo;
	}
	
	public int compareTo(Object other)
	{
		BallotNumber that = (BallotNumber)other;
		if(this.ballotNo < that.getBallotNo())
			return -1;
		else if (this.ballotNo > that.getBallotNo())
			return 1;
		else{
			if(this.nodeNo < that.getNodeNo())
				return -1;
			else if(this.nodeNo > that.getNodeNo())
				return 1;
			else return 0;
		}
	}
	
}
