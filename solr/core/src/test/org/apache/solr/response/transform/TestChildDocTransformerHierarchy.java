/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.response.transform;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestChildDocTransformerHierarchy extends SolrTestCaseJ4 {

  private static AtomicInteger counter = new AtomicInteger();
  private static final String[] types = {"donut", "cake"};
  private static final String[] ingredients = {"flour", "cocoa", "vanilla"};
  private static final Iterator<String> ingredientsCycler = Iterables.cycle(ingredients).iterator();
  private static final String[] names = {"Yaz", "Jazz", "Costa"};
  private static final String[] fieldsToRemove = {"_nest_parent_", "_nest_path_", "_root_"};

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema-nest.xml"); // use "nest" schema
  }

  @After
  public void after() throws Exception {
    clearIndex();
    assertU(commit());
    counter.set(0); // reset id counter
  }

  @Test
  public void testParentFilterJSON() throws Exception {
    indexSampleData(10);
    String[] tests = new String[] {
        "/response/docs/[0]/type_s==donut",
        "/response/docs/[0]/toppings/[0]/type_s==Regular",
        "/response/docs/[0]/toppings/[1]/type_s==Chocolate",
        "/response/docs/[0]/toppings/[0]/ingredients/[0]/name_s==cocoa",
        "/response/docs/[0]/toppings/[1]/ingredients/[1]/name_s==cocoa",
        "/response/docs/[0]/lonely/test_s==testing",
        "/response/docs/[0]/lonely/lonelyGrandChild/test2_s==secondTest",
    };

    try(SolrQueryRequest req = req("q", "type_s:donut", "sort", "id asc", "fl", "*, _nest_path_, [child]")) {
      BasicResultContext res = (BasicResultContext) h.queryAndResponse("/select", req).getResponse();
      Iterator<SolrDocument> docsStreamer = res.getProcessedDocuments();
      while (docsStreamer.hasNext()) {
        SolrDocument doc = docsStreamer.next();
        cleanSolrDocumentFields(doc);
        int currDocId = Integer.parseInt((doc.getFirstValue("id")).toString());
        assertEquals("queried docs are not equal to expected output for id: " + currDocId, fullNestedDocTemplate(currDocId), doc.toString());
      }
    }

    assertJQ(req("q", "type_s:donut",
        "sort", "id asc",
        "fl", "*, _nest_path_, [child]"),
        tests);
  }

  @Test
  public void testExactPath() throws Exception {
    indexSampleData(2);
    String[] tests = {
        "/response/numFound==4",
        "/response/docs/[0]/_nest_path_=='toppings#0'",
        "/response/docs/[1]/_nest_path_=='toppings#0'",
        "/response/docs/[2]/_nest_path_=='toppings#1'",
        "/response/docs/[3]/_nest_path_=='toppings#1'",
    };

    assertJQ(req("q", "_nest_path_:*toppings/",
        "sort", "_nest_path_ asc",
        "fl", "*, _nest_path_"),
        tests);

    assertJQ(req("q", "+_nest_path_:\"toppings/\"",
        "sort", "_nest_path_ asc",
        "fl", "*, _nest_path_"),
        tests);
  }

  @Test
  public void testChildFilterJSON() throws Exception {
    indexSampleData(10);
    String[] tests = new String[] {
        "/response/docs/[0]/type_s==donut",
        "/response/docs/[0]/toppings/[0]/type_s==Regular",
    };

    assertJQ(req("q", "type_s:donut",
        "sort", "id asc",
        "fl", "*,[child childFilter='toppings/type_s:Regular']"),
        tests);
  }

  @Test
  public void testGrandChildFilterJSON() throws Exception {
    indexSampleData(10);
    String[] tests = new String[] {
        "/response/docs/[0]/type_s==donut",
        "/response/docs/[0]/toppings/[0]/ingredients/[0]/name_s==cocoa"
    };

    try(SolrQueryRequest req = req("q", "type_s:donut", "sort", "id asc",
        "fl", "*,[child childFilter='toppings/ingredients/name_s:cocoa']")) {
      BasicResultContext res = (BasicResultContext) h.queryAndResponse("/select", req).getResponse();
      Iterator<SolrDocument> docsStreamer = res.getProcessedDocuments();
      while (docsStreamer.hasNext()) {
        SolrDocument doc = docsStreamer.next();
        cleanSolrDocumentFields(doc);
        int currDocId = Integer.parseInt((doc.getFirstValue("id")).toString());
        assertEquals("queried docs are not equal to expected output for id: " + currDocId, grandChildDocTemplate(currDocId), doc.toString());
      }
    }



    assertJQ(req("q", "type_s:donut",
        "sort", "id asc",
        "fl", "*,[child childFilter='toppings/ingredients/name_s:cocoa']"),
        tests);
  }

  @Test
  public void testSingularChildFilterJSON() throws Exception {
    indexSampleData(10);
    String[] tests = new String[] {
        "/response/docs/[0]/type_s==cake",
        "/response/docs/[0]/lonely/test_s==testing",
        "/response/docs/[0]/lonely/lonelyGrandChild/test2_s==secondTest"
    };

    assertJQ(req("q", "type_s:cake",
        "sort", "id asc",
        "fl", "*,[child childFilter='lonely/lonelyGrandChild/test2_s:secondTest']"),
        tests);
  }

  private void indexSampleData(int numDocs) throws Exception {
    for(int i = 0; i < numDocs; ++i) {
      updateJ(generateDocHierarchy(i), params("update.chain", "nested"));
    }
    assertU(commit());
  }

  private static String id() {
    return "" + counter.incrementAndGet();
  }

  private static void cleanSolrDocumentFields(SolrDocument input) {
    for(String fieldName: fieldsToRemove) {
      input.removeFields(fieldName);
    }
    for(Map.Entry<String, Object> field: input) {
      Object val = field.getValue();
      if(val instanceof Collection) {
        Object newVals = ((Collection) val).stream().map((item) -> (cleanIndexableField(item)))
            .collect(Collectors.toList());
        input.setField(field.getKey(), newVals);
        continue;
      }
      input.setField(field.getKey(), cleanIndexableField(field.getValue()));
    }
  }

  private static Object cleanIndexableField(Object field) {
    if(field instanceof IndexableField) {
      return ((IndexableField) field).stringValue();
    } else if(field instanceof SolrDocument) {
      cleanSolrDocumentFields((SolrDocument) field);
    }
    return field;
  }

  private static String grandChildDocTemplate(int id) {
    int docNum = id / 8; // the index of docs sent to solr in the AddUpdateCommand. e.g. first doc is 0
    return "SolrDocument{id="+ id + ", type_s=" + types[docNum % types.length] + ", name_s=" + names[docNum % names.length] + ", " +
        "toppings=[SolrDocument{id=" + (id + 3) + ", type_s=Regular, " +
        "ingredients=[SolrDocument{id=" + (id + 4) + ", name_s=cocoa}]}, " +
        "SolrDocument{id=" + (id + 5) + ", type_s=Chocolate, " +
        "ingredients=[SolrDocument{id=" + (id + 6) + ", name_s=cocoa}, " +
        "SolrDocument{id=" + (id + 7) + ", name_s=cocoa}]}]}";
  }

  private static String fullNestedDocTemplate(int id) {
    int docNum = id / 8; // the index of docs sent to solr in the AddUpdateCommand. e.g. first doc is 0
    boolean doubleIngredient = docNum % 2 == 0;
    String currIngredient = doubleIngredient ? ingredients[1]: ingredientsCycler.next();
    return "SolrDocument{id=" + id + ", type_s=" + types[docNum % types.length] + ", name_s=" + names[docNum % names.length] + ", " +
        "lonely=SolrDocument{id=" + (id + 1) + ", test_s=testing, " +
        "lonelyGrandChild=SolrDocument{id=" + (id + 2) + ", test2_s=secondTest}}, " +
        "toppings=[SolrDocument{id=" + (id + 3) + ", type_s=Regular, " +
        "ingredients=[SolrDocument{id=" + (id + 4) + ", name_s=" + currIngredient + "}]}, " +
        "SolrDocument{id=" + (id + 5) + ", type_s=Chocolate, ingredients=[SolrDocument{id=" + (id + 6) + ", name_s=cocoa}, " +
        "SolrDocument{id=" + (id + 7) + ", name_s=cocoa}]}]}";
  }

  private static String generateDocHierarchy(int i) {
    boolean doubleIngredient = i % 2 == 0;
    String currIngredient = doubleIngredient ? ingredients[1]: ingredientsCycler.next();
    return "{\n" +
              "\"add\": {\n" +
                "\"doc\": {\n" +
                  "\"id\": " + id() + ", \n" +
                  "\"type_s\": \""+ types[i % types.length] + "\", \n" +
                  "\"lonely\": {\"id\": " + id() + ", \"test_s\": \"testing\", \"lonelyGrandChild\": {\"id\": " + id() + ", \"test2_s\": \"secondTest\"}}, \n" +
                  "\"name_s\": " + names[i % names.length] +
                  "\"toppings\": [ \n" +
                    "{\"id\": " + id() + ", \"type_s\":\"Regular\"," +
                      "\"ingredients\": [{\"id\": " + id() + "," +
                        "\"name_s\": \"" + currIngredient + "\"}]" +
                    "},\n" +
                    "{\"id\": " + id() + ", \"type_s\":\"Chocolate\"," +
                      "\"ingredients\": [{\"id\": " + id() + "," +
                        "\"name_s\": \"" + ingredients[1] + "\"}," +
                        "{\"id\": " + id() + ",\n" + "\"name_s\": \"" + ingredients[1] +"\"" +
                        "}]" +
                  "}]\n" +
                "}\n" +
              "}\n" +
            "}";
  }
}
