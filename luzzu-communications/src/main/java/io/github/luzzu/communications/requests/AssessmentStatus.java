package io.github.luzzu.communications.requests;

public enum AssessmentStatus {

	INPROGRESS {
	    public String toString() {
	        return "Assessment In Progress";
	    }
	}, SUCCESSFUL {
	    public String toString() {
	        return "Successful Assessment";
	    }
	}, FAILED {
	    public String toString() {
	        return "Failed Assessment";
	    }
	}, CANCELLED {
	    public String toString() {
	        return "Assessment Aborted by User";
	    }
	}
}
