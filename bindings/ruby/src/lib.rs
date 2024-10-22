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

// We will use `ocore::Xxx` to represents all types from opendal rust core.
// This also aligns with Python binding.
pub use ::opendal as ocore;

use magnus::{function, method, class, exception, prelude::*, RModule, Error, Ruby};

// mod capability;
// pub use capability::*;
// mod layers;
// pub use layers::*;
// mod lister;
// pub use lister::*;
mod metadata;
pub use metadata::*;
mod operator;
pub use operator::*;
mod io;
pub use io::*;
// mod utils;
// pub use utils::*;
// mod errors;
// pub use errors::*;
// mod options;
// pub use options::*;


fn format_magnus_error(err: ocore::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

/// Apache OpenDAL™ Ruby binding
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let gem_module = ruby.define_module("OpenDAL")?;
    let _ = register_operator(&gem_module);
    let _ = register_metadata(&gem_module);
    let _ = register_io(&gem_module);

    Ok(())
}


fn register_operator(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("Operator", class::object())?;
    class.define_singleton_method("new", function!(Operator::new, 2))?;
    class.define_method("read", method!(Operator::read, 1))?;
    class.define_method("write", method!(Operator::write, 2))?;
    class.define_method("stat", method!(Operator::stat, 1))?;

    Ok(())
}

fn register_metadata(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("Metadata", class::object())?;
    class.define_method(
        "content_disposition",
        method!(Metadata::content_disposition, 0),
    )?;
    class.define_method("content_length", method!(Metadata::content_length, 0))?;
    class.define_method("content_md5", method!(Metadata::content_md5, 0))?;
    class.define_method("content_type", method!(Metadata::content_type, 0))?;
    class.define_method("etag", method!(Metadata::etag, 0))?;
    class.define_method("is_file?", method!(Metadata::is_file, 0))?;
    class.define_method("is_dir?", method!(Metadata::is_dir, 0))?;

    Ok(())
}

fn register_io(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("FileIO", class::object())?;
    // class.define_singleton_method("new", function!(FileIO::new, 2))?;
    // class.define_method("read", method!(File::read, 1))?;
    // class.define_method("write", method!(File::write, 2))?;
    // class.define_method("tell", method!(File::tell, 1))?;

    Ok(())
}