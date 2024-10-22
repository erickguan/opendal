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


use std::collections::HashMap;
use std::str::FromStr;

use magnus::{function, method, class, exception, prelude::*, RModule, Error, Ruby};

use crate::*;

#[magnus::wrap(class = "OpenDAL::Operator", free_immediately, size)]
#[derive(Clone, Debug)]
pub struct Operator(ocore::BlockingOperator);

fn format_magnus_error(err: ocore::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

impl Operator {
    pub fn new(scheme: String, options: Option<HashMap<String, String>>) -> Self {
        let scheme = ocore::Scheme::from_str(&scheme)
            .map_err(|err| {
                ocore::Error::new(ocore::ErrorKind::Unexpected, "unsupported scheme").set_source(err)
            })
            .map_err(format_magnus_error)?;
        let options = options.unwrap_or_default();

        let op = ocore::Operator::via_iter(scheme, options)
            .map_err(format_magnus_error)?
            .blocking();
        Ok(Operator(op))
    }

    /// Read the whole path into string.
    pub fn read(&self, path: String) -> Result<RString, Error> {
        let bytes = self.0.read(&path).map_err(format_magnus_error)?;
        Ok(RString::from_slice(&bytes.to_vec()))
    }

    /// Write string into given path.
    pub fn write(&self, path: String, bs: RString) -> Result<(), Error> {
        self.0
            .write(&path, bs.to_bytes())
            .map_err(format_magnus_error)
    }

    /// Get current path's metadata **without cache** directly.
    pub fn stat(&self, path: String) -> Result<Metadata, Error> {
        self.0
            .stat(&path)
            .map_err(format_magnus_error)
            .map(|info| Metadata::new(info))
    }
}

pub fn register_operator(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("Operator", class::object())?;
    class.define_singleton_method("new", function!(Operator::new, 2))?;
    class.define_method("read", method!(Operator::read, 1))?;
    class.define_method("write", method!(Operator::write, 2))?;
    class.define_method("stat", method!(Operator::stat, 1))?;

    Ok(())
}
