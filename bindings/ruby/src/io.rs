// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::*;


use std::collections::HashMap;
use std::str::FromStr;

use magnus::class;
use magnus::define_module;
use magnus::error::Result;
use magnus::exception;
use magnus::function;
use magnus::method;
use magnus::prelude::*;
use magnus::Error;
use magnus::RString;

/// Pseudo `IO` class via OpenDAL.
///
/// # Differences to `IO`
/// - doesn't support all IO methods.
pub struct FileIO(FileState);

enum FileState {
    Reader(ocore::StdReader),
    Writer(ocore::StdWriter),
    Closed,
}

impl FileIO {
    pub fn new_reader(reader: ocore::StdReader) -> Self {
        Self(FileState::Reader(reader))
    }

    pub fn new_writer(writer: ocore::BlockingWriter) -> Self {
        Self(FileState::Writer(writer.into_std_write()))
    }

    // Read and return at most size bytes, or if size is not given, until EOF.
    // pub fn read

    // Write bytes into the file.
    // pub fn write

    // Return the current stream position.
    // pub fn tell(&mut self)

    // pub fn pos=
    
    // pub fn seek
    
    // pub fn rewind

    // pub fn close

    // pub fn close_read

    // pub fn close_write

    // pub fn closed?

    // pub fn eof?
}