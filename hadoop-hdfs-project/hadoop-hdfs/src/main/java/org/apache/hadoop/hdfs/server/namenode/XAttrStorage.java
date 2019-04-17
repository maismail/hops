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

import java.util.List;

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.INode;

import com.google.common.collect.ImmutableList;

/**
 * XAttrStorage is used to read and set xattrs for an inode.
 */
@InterfaceAudience.Private
public class XAttrStorage {
  
  /**
   * Reads an existing extended attribute of inode.
   * @param inode INode to read.
   * @param attr XAttr to read.
   * @return the existing XAttr.
   */
  public static XAttr readINodeXAttr(INode inode, XAttr attr)
      throws TransactionContextException, StorageException {
    List<XAttr> attrs = readINodeXAttrs(inode, Lists.newArrayList(attr));
    if(attrs == null || attrs.isEmpty())
      return null;
    return attrs.get(0);
  }
  
  /**
   * Reads an existing extended attribute of inode.
   * @param inode INode to read.
   * @param attrs List of XAttrs to read.
   * @return the existing list of XAttrs.
   */
  public static List<XAttr> readINodeXAttrs(INode inode, List<XAttr> attrs)
      throws TransactionContextException, StorageException {
    XAttrFeature f = inode.getXAttrFeature();
    if(f == null){
      inode.addXAttrFeature(new XAttrFeature(inode.getId()));
    }
  
    if(attrs == null || attrs.isEmpty()){
      return f.getXAttrs();
    }else{
      return f.getXAttr(attrs);
    }
  }
  
  /**
   * Update xattr of inode.
   * @param inode Inode to update.
   * @param xAttr the xAttr to update.
   */
  public static void updateINodeXAttr(INode inode, XAttr xAttr)
      throws TransactionContextException, StorageException {
    if(inode.getXAttrFeature() == null){
      inode.addXAttrFeature(new XAttrFeature(inode.getId()));
    }
    
    inode.getXAttrFeature().addXAttr(xAttr);
  }
  
  /**
   * Remove xattr from inode.
   * @param inode Inode to update.
   * @param xAttr the xAttr to remove.
   */
  public static void removeINodeXAttr(INode inode, XAttr xAttr)
      throws TransactionContextException, StorageException {
    if(inode.getXAttrFeature() == null){
      inode.addXAttrFeature(new XAttrFeature(inode.getId()));
    }
  
    inode.getXAttrFeature().removeXAttr(xAttr);
  }
}
