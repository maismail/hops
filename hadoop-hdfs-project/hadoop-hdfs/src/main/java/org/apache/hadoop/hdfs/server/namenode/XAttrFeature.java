/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

/**
 * Feature for extended attributes.
 */
@InterfaceAudience.Private
public class XAttrFeature implements INode.Feature {

  private final long inodeId;
  
  public XAttrFeature(long inodeId){
    this.inodeId = inodeId;
  }
  
  public XAttrFeature(ImmutableList<XAttr> xAttrs, long inodeId)
      throws TransactionContextException, StorageException {
    this.inodeId = inodeId;
    for(XAttr attr: xAttrs){
      EntityManager.add(convertXAttrtoStored(attr));
    }
  }

  public XAttr getXAttr(XAttr attr)
      throws StorageException, TransactionContextException {
    StoredXAttr storedXAttr = EntityManager.find(StoredXAttr.Finder.ByPrimaryKey,
        getPrimaryKey(attr));
    if(storedXAttr == null)
      return null;
    return convertStoredtoXAttr(storedXAttr);
  }
  
  public void addXAttr(XAttr attr)
      throws TransactionContextException, StorageException {
    StoredXAttr storedXAttr = convertXAttrtoStored(attr);
    EntityManager.add(storedXAttr);
  }
  
  public void removeXAttr(XAttr attr)
      throws TransactionContextException, StorageException {
    StoredXAttr storedXAttr = convertXAttrtoStored(attr);
    EntityManager.remove(storedXAttr);
  }
  
  public ImmutableList<XAttr> getXAttrs()
      throws TransactionContextException, StorageException {
    //return xAttrs;
    Collection<StoredXAttr> extendedAttributes =
        EntityManager.findList(StoredXAttr.Finder.ByInodeId, inodeId);
    List<XAttr> attrs =
        Lists.newArrayListWithExpectedSize(extendedAttributes.size());
    for(StoredXAttr attr : extendedAttributes){
      attrs.add(convertStoredtoXAttr(attr));
    }
    return ImmutableList.copyOf(attrs);
  }
  
  private XAttr convertStoredtoXAttr(StoredXAttr attr){
    XAttr.Builder builder = new XAttr.Builder();
    builder.setName(attr.getName());
    builder.setNameSpace(XAttr.NameSpace.values()[attr.getNamespace()]);
    builder.setValue(attr.getValueBytes());
    return builder.build();
  }
  
  private StoredXAttr convertXAttrtoStored(XAttr attr){
    return new StoredXAttr(inodeId, attr.getNameSpaceByte(), attr.getName(),
        attr.getValue());
  }
  
  private StoredXAttr.PrimaryKey getPrimaryKey(XAttr attr){
    return new StoredXAttr.PrimaryKey(inodeId, attr.getNameSpaceByte(),
        attr.getName());
  }
 
}
