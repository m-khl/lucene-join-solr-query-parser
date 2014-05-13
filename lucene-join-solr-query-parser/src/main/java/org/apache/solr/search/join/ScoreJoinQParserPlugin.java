package org.apache.solr.search.join;
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


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.RefCounted;


public class ScoreJoinQParserPlugin extends QParserPlugin {
  
   static class OtherCoreJoinQuery extends SameCoreJoinQuery {
    private final String fromIndex;
    private final long fromCoreOpenTime;
    
    public OtherCoreJoinQuery(Query fromQuery, String fromField,
        String fromIndex, long fromCoreOpenTime, ScoreMode scoreMode,
        String toField) {
      super(fromQuery, fromField, toField,scoreMode);
      this.fromIndex= fromIndex;
      this.fromCoreOpenTime = fromCoreOpenTime;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      
      CoreContainer container = info.getReq().getCore().getCoreDescriptor().getCoreContainer();

      final SolrCore fromCore = container.getCore(fromIndex);

      if (fromCore == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
      }
      RefCounted<SolrIndexSearcher> fromHolder = null;
        fromHolder = fromCore.getRegisteredSearcher();
        final Query joinQuery;
        try{
        joinQuery = JoinUtil.createJoinQuery(fromField, multipleValuesPerDocument , 
          toField, fromQuery, fromHolder.get(), scoreMode);
        }finally{
          fromCore.close();
          fromHolder.decref();
        }
        joinQuery.setBoost(getBoost());
      return joinQuery.rewrite(reader);
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + (int) (fromCoreOpenTime ^ (fromCoreOpenTime >>> 32));
      result = prime * result
          + ((fromIndex == null) ? 0 : fromIndex.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      OtherCoreJoinQuery other = (OtherCoreJoinQuery) obj;
      if (fromCoreOpenTime != other.fromCoreOpenTime) return false;
      if (fromIndex == null) {
        if (other.fromIndex != null) return false;
      } else if (!fromIndex.equals(other.fromIndex)) return false;
      return true;
    }

    @Override
    public String toString() {
      return "OtherCoreJoinQuery [fromIndex=" + fromIndex
          + ", fromCoreOpenTime=" + fromCoreOpenTime + " extends "
          + super.toString() + "]"; 
    }
  }

  static class SameCoreJoinQuery extends Query {
    protected final Query fromQuery;
    protected final ScoreMode scoreMode;
    protected final String fromField;
    protected final String toField;
    
    SameCoreJoinQuery(Query fromQuery, String fromField, String toField,
        ScoreMode scoreMode) {
      this.fromQuery = fromQuery;
      this.scoreMode = scoreMode;
      this.fromField = fromField;
      this.toField = toField;
    }
    
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      final Query jq = JoinUtil.createJoinQuery(fromField, multipleValuesPerDocument , 
          toField, fromQuery, info.getReq().getSearcher(), scoreMode);
      jq.setBoost(getBoost());
    return jq.rewrite(reader);
    }

    
    @Override
    public String toString() {
      return "SameCoreJoinQuery [fromQuery=" + fromQuery + ", fromField="
          + fromField + ", toField=" + toField + ", scoreMode=" + scoreMode
          + ", boost=" + getBoost() + "]";
    }


    @Override
    public String toString(String field) {
      return toString();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + ((fromField == null) ? 0 : fromField.hashCode());
      result = prime * result
          + ((fromQuery == null) ? 0 : fromQuery.hashCode());
      result = prime * result
          + ((scoreMode == null) ? 0 : scoreMode.hashCode());
      result = prime * result + ((toField == null) ? 0 : toField.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      SameCoreJoinQuery other = (SameCoreJoinQuery) obj;
      if (fromField == null) {
        if (other.fromField != null) return false;
      } else if (!fromField.equals(other.fromField)) return false;
      if (fromQuery == null) {
        if (other.fromQuery != null) return false;
      } else if (!fromQuery.equals(other.fromQuery)) return false;
      if (scoreMode != other.scoreMode) return false;
      if (toField == null) {
        if (other.toField != null) return false;
      } else if (!toField.equals(other.toField)) return false;
      return true;
    }
  }

  public static final String NAME = "scorejoin";
  final static Map<String, ScoreMode> lowercase = new HashMap<String, ScoreMode>(){
    {
      for(ScoreMode s : ScoreMode.values()){
        put(s.name().toLowerCase(), s);
        put(s.name(), s);
      }
    }
  };
  // TODO extract param
  private final static boolean multipleValuesPerDocument = true;
  
  @Override
  public void init(NamedList args) {
  }
  


  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        final String fromField = getParam("from");
        String fromIndex = getParam("fromIndex");
        final String toField = getParam("to");
        final ScoreMode scoreMode = parseScore();
        String v = localParams.get("v");
        final Query fromQuery;
        
        QParser fromQueryParser = subQuery(v, null);
        fromQuery = fromQueryParser.getQuery();

        final Query q = createQuery(fromField, fromQuery, fromIndex, toField, scoreMode);
        String b = localParams.get("b");
        if(b!=null){
            final float boost = Float.parseFloat(b);
            q.setBoost(boost);
        }
        return q;
      }

    private Query createQuery(final String fromField, final Query fromQuery,
            String fromIndex, final String toField, final ScoreMode scoreMode) {
        if (fromIndex != null && !fromIndex.equals(req.getCore().getCoreDescriptor().getName()) ) {
            CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();

            final SolrCore fromCore = container.getCore(fromIndex);
            RefCounted<SolrIndexSearcher> fromHolder = null;

            if (fromCore == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
            }

            long fromCoreOpenTime = 0;
            LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);
            try {
              fromHolder = fromCore.getRegisteredSearcher();
              if (fromHolder != null) {
                fromCoreOpenTime = fromHolder.get().getOpenTime();
              }
              return new OtherCoreJoinQuery(fromQuery,  fromField, fromIndex, fromCoreOpenTime, scoreMode, toField);
            } finally {
              otherReq.close();
              fromCore.close(); 
              if (fromHolder != null) fromHolder.decref();
            }
          } else { 
            return new SameCoreJoinQuery(fromQuery, fromField, toField, scoreMode);
          }
    }

      private ScoreMode parseScore() {
        
        String score = getParam("score");
        final ScoreMode scoreMode =
            (score==null || score.equals("")) ? ScoreMode.None: lowercase.get(score);
        if(scoreMode==null){
          throw new IllegalArgumentException("Unable to parse ScoreMode from: "+score);
        }
        return scoreMode;
      }
    };
  }
}
