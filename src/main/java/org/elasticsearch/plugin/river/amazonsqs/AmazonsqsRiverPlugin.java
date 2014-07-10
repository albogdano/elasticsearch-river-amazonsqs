/*
 * Copyright 2013 Alex Bogdanovski [alex@erudika.com].
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can reach the author at: https://github.com/albogdano
 */
package org.elasticsearch.plugin.river.amazonsqs;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.amazonsqs.AmazonsqsRiverModule;

/**
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class AmazonsqsRiverPlugin extends AbstractPlugin {

    @Inject public AmazonsqsRiverPlugin() {
    }

    @Override public String name() {
        return "river-amazonsqs";
    }

    @Override public String description() {
        return "River AmazonSQS Plugin";
    }

    public void onModule(RiversModule module){
        module.registerRiver("amazonsqs", AmazonsqsRiverModule.class);
    }

}
