/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.adamfisher.nifi.processors.czds;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.icann.czds.sdk.model.ClientConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"czds", "icann", "zonefile", "input", "get", "fetch", "source", "ingest"})
@CapabilityDescription("Creates FlowFiles by downloading zone files from ICANN's Central Zone Data Service (CZDS) program.")
@WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on disk")
@Restricted(
    restrictions = {
        @Restriction(
            requiredPermission = RequiredPermission.READ_FILESYSTEM,
            explanation = "Provides operator the ability to read from any file that NiFi has access to."),
        @Restriction(
            requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
            explanation = "Provides operator the ability to delete any file that NiFi has access to.")
    }
)
public class GetCentralZoneDataServiceFile extends AbstractProcessor {
    public static final PropertyDescriptor AUTH_URL = new PropertyDescriptor
            .Builder().name("auth-url")
            .displayName("Authentication URL")
            .description("The authentication REST endpoint base URL.")
            .defaultValue("https://account-api.icann.org")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    public static final PropertyDescriptor CZDS_URL = new PropertyDescriptor
            .Builder().name("czds-url")
            .displayName("CZDS URL")
            .description("The CZDS REST endpoint base URL.")
            .defaultValue("https://czds-api.icann.org")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder().name("czds-username")
            .displayName("Username")
            .description("Your CZDS username")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("czds-password")
            .displayName("Password")
            .description("Your CZDS password")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOP_LEVEL_DOMAINS = new PropertyDescriptor
            .Builder().name("top-level-domains")
            .displayName("Top Level Domains")
            .description("Specify the TLD(s) you want to download zone file(s) for. Comma separate multiple TLDs. " +
                    "By default, all APPROVED zone files will be downloaded.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor
            .Builder().name("directory")
            .displayName("Directory")
            .description("Specify the directory where the file(s) will be saved.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .build();

    public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor
            .Builder().name("Keep Source File")
            .description("If true, the file is not deleted after it has been copied to the Content Repository; "
                    + "this causes the file to be picked up continually and is useful for testing purposes.  "
                    + "If not keeping original NiFi will need write permissions on the directory it is pulling "
                    + "from otherwise it will ignore the file.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All files are transferred to the success relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private final AtomicReference<NiFiZoneDownloadClient> client = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(AUTH_URL);
        descriptors.add(CZDS_URL);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(TOP_LEVEL_DOMAINS);
        descriptors.add(DIRECTORY);
        descriptors.add(KEEP_SOURCE_FILE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final ClientConfiguration clientConfiguration = ClientConfiguration.getInstance();
        clientConfiguration.setAuthenticationBaseUrl(context.getProperty(AUTH_URL).evaluateAttributeExpressions().getValue());
        clientConfiguration.setCzdsBaseUrl(context.getProperty(CZDS_URL).evaluateAttributeExpressions().getValue());
        clientConfiguration.setUserName(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue());
        clientConfiguration.setPassword(context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue());
        clientConfiguration.setWorkingDirectory(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue());
        clientConfiguration.setUserName(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue());
        client.set(new NiFiZoneDownloadClient(clientConfiguration));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        try {
            final boolean onlyDownloadSpecificZoneFiles = context.getProperty(TOP_LEVEL_DOMAINS).isSet();
            final boolean keepSourceFile = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
            final NiFiZoneDownloadClient czdsClient = client.get();

            final Set<String> requestedZones = onlyDownloadSpecificZoneFiles
                    ? new HashSet<>(Arrays.asList(context.getProperty(TOP_LEVEL_DOMAINS).evaluateAttributeExpressions().getValue().split(",")))
                    : czdsClient.getDownloadURLs();

            for(String requestedZone : requestedZones) {
                try {
                    FlowFile flowFile = session.create();
                    final long importStart = System.nanoTime();
                    File zoneFile = onlyDownloadSpecificZoneFiles ? czdsClient.downloadZoneFile(requestedZone) : czdsClient.getZoneFile(requestedZone);
                    flowFile = session.importFrom(zoneFile.toPath(), keepSourceFile, flowFile);
                    final long importNanos = System.nanoTime() - importStart;
                    final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
                    flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), zoneFile.getName());

                    session.getProvenanceReporter().receive(flowFile, zoneFile.toURI().toString(), importMillis);
                    session.transfer(flowFile, REL_SUCCESS);
                    logger.info("added {} to flow", new Object[]{flowFile});
                    session.commit();
                } catch (Exception e) {
                    logger.error("Failed to retrieve zone file for {} because {}", new Object[]{requestedZone, e});
                }
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve ICANN CZDS zone files due to {}", e);
        }
    }
}
