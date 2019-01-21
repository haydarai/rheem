package org.qcri.rheem.experiment.crocopr.udf;

import org.qcri.rheem.experiment.ExperimentException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserDBPedia implements Serializable {

    private String pattern = "<http://dbpedia.org/resource/([^>]+)>\\s+<http://dbpedia.org/ontology/wikiPageWikiLink>\\s+<http://dbpedia.org/resource/([^>]+)>\\s+\\.";
    private Pattern structure;


    public ParserDBPedia(){
        this.structure = Pattern.compile(pattern);
    }

    public String[] components(String dbpedia_line){
        Matcher matcher = this.structure.matcher(dbpedia_line);

        if(! matcher.matches() || matcher.groupCount() != 2 ){
            return null;
        }
        try {
            String[] matchs = new String[2];
            matchs[0] = matcher.group(1);
            matchs[1] = matcher.group(2);
            return matchs;
        }catch (Exception e){
            throw new ExperimentException(dbpedia_line, e);
        }
    }


    public static void main(String... args){
        ParserDBPedia p = new ParserDBPedia();
        System.out.println(Arrays.toString(p.components("<http://dbpedia.org/resource/Nucleic_acid> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://dbpedia.org/resource/Plasmid> .")));
    }
}

