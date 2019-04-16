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
package io.hops.transaction.context;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XAttrContext extends BaseEntityContext<StoredXAttr.PrimaryKey,
    StoredXAttr> {
  
  private final XAttrDataAccess<StoredXAttr> dataAccess;
  private final Map<Long, Collection<StoredXAttr>> xAttrsByInodeId =
      new HashMap<>();
  
  public XAttrContext(XAttrDataAccess<StoredXAttr> dataAccess){
    this.dataAccess = dataAccess;
  }
  
  @Override
  StoredXAttr.PrimaryKey getKey(StoredXAttr storedXAttr) {
    return storedXAttr.getPrimaryKey();
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }
  
  @Override
  public Collection<StoredXAttr> findList(FinderType<StoredXAttr> finder,
      Object... params) throws TransactionContextException, StorageException {
    StoredXAttr.Finder xfinder = (StoredXAttr.Finder) finder;
    switch (xfinder){
      case ByInodeId:
        return findByInodeId(xfinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }
  
  private Collection<StoredXAttr> findByInodeId(StoredXAttr.Finder finder,
      Object[] params) throws StorageException {
    final long inodeId = (Long) params[0];
    Collection<StoredXAttr> results = null;
    if(xAttrsByInodeId.containsKey(inodeId)){
      hit(finder, results, "inodeId", inodeId);
      return xAttrsByInodeId.get(inodeId);
    }else{
      results = dataAccess.getXAttrsByInodeId(inodeId);
      xAttrsByInodeId.put(inodeId, results);
      gotFromDB(results);
      miss(finder, results, "inodeId", inodeId);
    }
    return results;
  }
  
  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    xAttrsByInodeId.clear();
  }
}
