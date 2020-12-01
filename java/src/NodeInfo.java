public class NodeInfo {
	
	private String ip;
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getLowRange() {
		return lowRange;
	}
	public void setLowRange(String lowRange) {
		this.lowRange = lowRange;
	}
	public String getUpperRange() {
		return upperRange;
	}
	public void setUpperRange(String upperRange) {
		this.upperRange = upperRange;
	}
	private String port;
	private String lowRange;
	private String upperRange;

}