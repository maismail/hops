/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.lock;

import com.google.common.collect.Lists;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XAttrLock extends Lock{
  
  private final List<XAttr> attrs;
  
  public XAttrLock(List<XAttr> attrs){
    this.attrs = attrs;
  }
  
  public XAttrLock(XAttr attr){
    if(attr != null) {
      this.attrs = new ArrayList<>();
      this.attrs.add(attr);
    }else{
      this.attrs = null;
    }
  }
  
  public XAttrLock(){
    this.attrs = null;
  }
  
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);
    for(INode inode : inodeLock.getTargetINodes()){
      
      //read all xattrs
      if(attrs == null) {
        //optimization; check the num_xattrs in the inode to avoid database
        EntityManager.findList(StoredXAttr.Finder.ByInodeId, inode.getId());
      }else{
        EntityManager.findList(StoredXAttr.Finder.ByPrimaryKeyBatch,
            convert(inode.getId(), attrs));
      }
    }
  }
  
  @Override
  protected Type getType() {
    return Type.XAttr;
  }
  
  private List<StoredXAttr.PrimaryKey> convert(long inodeId, List<XAttr> attrs){
    List<StoredXAttr.PrimaryKey> pks =
        Lists.newArrayListWithExpectedSize(attrs.size());
    for(XAttr attr : attrs){
      pks.add(new StoredXAttr.PrimaryKey(inodeId, attr.getNameSpaceByte(),
          attr.getName()));
    }
    return pks;
  }
}
