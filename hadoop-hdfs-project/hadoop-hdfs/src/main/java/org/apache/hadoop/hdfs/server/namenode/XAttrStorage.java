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
   * Reads the existing extended attributes of an inode.
   * @param inode INode to read.
   * @return List<XAttr> <code>XAttr</code> list.
   */
  public static List<XAttr> readINodeXAttrs(INode inode)
      throws TransactionContextException, StorageException {
    XAttrFeature f = inode.getXAttrFeature();
    return f == null ? ImmutableList.<XAttr> of() : f.getXAttrs();
  }
  
  /**
   * Update xattrs of inode.
   * @param inode INode to update
   * @param xAttrs to update xAttrs.
   */
  public static void updateINodeXAttrs(INode inode, 
      List<XAttr> xAttrs)
      throws QuotaExceededException, TransactionContextException,
      StorageException {
    if (xAttrs == null || xAttrs.isEmpty()) {
      return;
    }
    
    ImmutableList<XAttr> newXAttrs = ImmutableList.copyOf(xAttrs);
    inode.addXAttrFeature(new XAttrFeature(newXAttrs, inode.getId()));
  }
}
