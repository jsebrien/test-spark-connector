package test.sparkconnector;

import java.util.UUID;

public class TestBean {

	private UUID id;
	private double x0;
	private double y0;

	public TestBean() {
	}

	public TestBean(UUID id, double x0, double y0) {
		super();
		this.id = id;
		this.x0 = x0;
		this.y0 = y0;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public double getX0() {
		return x0;
	}

	public void setX0(double x0) {
		this.x0 = x0;
	}

	public double getY0() {
		return y0;
	}

	public void setY0(double y0) {
		this.y0 = y0;
	}

	@Override
	public String toString() {
		return "TestBean [id=" + id + ", x0=" + x0 + ", y0=" + y0 + "]";
	}

}
