/*
 * Copyright 2018 Michaël Coquard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.micoq.elasticsearch;

import fr.micoq.unsafe.MappedMemory;

public class MappedIndexInputGuard {
  
  private MappedMemory memory;
  private long refCounter;
  
  public MappedIndexInputGuard(MappedMemory memory) {
    this.memory = memory;
    this.refCounter = 0L;
  }
  
  public void close() {
    if(this.refCounter > 0) {
      this.refCounter--;
      if(this.refCounter == 0)
        this.memory.close();
    }
  }
  
  public void open() {
    this.refCounter++;
  }
  
  public MappedMemory getMemory() {
    return this.memory;
  }
}
