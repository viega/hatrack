/*
 * Copyright © 2022 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           hatrack.h
 *  Description:    Single header file to pull in all functionality.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __HATRACK_H__
#define __HATRACK_H__

#include <hatrack/gate.h>

// Currently pulls in Crown.
#include <hatrack/dict.h>

// Currently pulls in Woolhat.
#include <hatrack/set.h>

#ifdef HATRACK_COMPILE_ALL_ALGORITHMS
#include <hatrack/tophat.h>
#include <hatrack/lohat-a.h>
#include <hatrack/lohat.h>
#include <hatrack/witchhat.h>
#include <hatrack/hihat.h>
#include <hatrack/oldhat.h>
#include <hatrack/tiara.h>
#include <hatrack/ballcap.h>
#include <hatrack/newshat.h>
#include <hatrack/swimcap.h>
#include <hatrack/duncecap.h>
#include <hatrack/refhat.h>
#endif

#include <hatrack/hash.h>
#include <hatrack/queue.h>
#include <hatrack/hq.h>
#include <hatrack/flexarray.h>
#include <hatrack/llstack.h>
#include <hatrack/stack.h>
#endif
