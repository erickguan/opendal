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

#[magnus::wrap(class = "OpenDAL::Metadata", free_immediately, size)]
pub struct Metadata(ocore::Metadata);

impl Metadata {
    pub fn new(metadata: ocore::Metadata) -> Self {
        Self(metadata)
    }
}

impl Metadata {
    /// Content-Disposition of this object
    pub fn content_disposition(&self) -> Option<&str> {
        self.0.content_disposition()
    }

    /// Content length of this entry.
    pub fn content_length(&self) -> u64 {
        self.0.content_length()
    }

    /// Content MD5 of this entry.
    pub fn content_md5(&self) -> Option<&str> {
        self.0.content_md5()
    }

    /// Content Type of this entry.
    pub fn content_type(&self) -> Option<&str> {
        self.0.content_type()
    }

    /// ETag of this entry.
    pub fn etag(&self) -> Option<&str> {
        self.0.etag()
    }

    /// Returns `True` if this is a file.
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    /// Returns `True` if this is a directory.
    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }
}
