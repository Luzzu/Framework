package io.github.luzzu.qualityproblems.old;

import java.util.concurrent.TimeUnit;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;

import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;


public class ProblemCollectionQuadTest {
	
	public static void main(String [] args) {
	
		Resource metric = ModelFactory.createDefaultModel().createResource("urn:MetricExample");
//		ProblemCollection<Quad> problemCollection = new ProblemCollectionQuadSlower(metric);

		int elementsToAdd = 500000;
		
		int warmUp = 5;
	    execute(elementsToAdd, metric, warmUp);
	    System.out.println("Warm Up Done");
//	    System.gc();
	    int itr = 10;
	    execute(elementsToAdd, metric, itr);
	}

	
    private static void execute(int size, Resource metric, int itr) {
        for (int cnt = 0; cnt < itr; cnt++) {
    			ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(metric);

            long start = System.nanoTime();
            for (int i = 0; i < size; i++) {
				Quad q = new Quad(null, new Triple(ResourceCommons.generateURI().asNode(), ResourceCommons.generateURI().asNode(), ResourceCommons.generateURI().asNode()));
				problemCollection.addProblem(q);
            }
            ((ProblemCollectionQuad)problemCollection).commit();
            long write = System.nanoTime() - start;
            problemCollection.cleanup();
    	    		System.gc();

            System.out.println(String.format("[%s] %s Write took %s ms, Write/tp :%s",
                    cnt, problemCollection.getClass().getName(),
                    TimeUnit.NANOSECONDS.toMillis(write), TimeUnit.SECONDS.toNanos(size) / write));
        }
    }

}
