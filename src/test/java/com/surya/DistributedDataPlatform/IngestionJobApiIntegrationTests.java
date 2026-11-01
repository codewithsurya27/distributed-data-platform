package com.surya.DistributedDataPlatform;

import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.http.MediaType;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.util.UUID;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest
@org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class IngestionJobApiIntegrationTests {

    private static final String JOBS_URL = "/api/v1/ingestion-jobs";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    @DynamicPropertySource
    static void configurePostgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private IngestionJobRepository repository;

    @BeforeEach
    void clearJobs() {
        repository.deleteAll();
    }

    @Test
    void createsMetadataOnlyJobAndRetrievesPersistedJob() throws Exception {
        MvcResult result = mockMvc.perform(post(JOBS_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"fileName":"customers.csv"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.fileName").value("customers.csv"))
                .andExpect(jsonPath("$.filePath").doesNotExist())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.totalRows").value(0))
                .andExpect(jsonPath("$.processedRows").value(0))
                .andExpect(jsonPath("$.failedRows").value(0))
                .andReturn();

        String path = URI.create(result.getResponse().getHeader("Location")).getPath();
        UUID id = UUID.fromString(path.substring(path.lastIndexOf('/') + 1));

        mockMvc.perform(get(JOBS_URL + "/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.fileName").value("customers.csv"));
    }

    @Test
    void createsJobWithOptionalFilePath() throws Exception {
        MvcResult result = mockMvc.perform(post(JOBS_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"fileName":"customers.csv","filePath":"uploads/customers.csv"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.filePath").value("uploads/customers.csv"))
                .andReturn();

        String path = URI.create(result.getResponse().getHeader("Location")).getPath();
        UUID id = UUID.fromString(path.substring(path.lastIndexOf('/') + 1));

        mockMvc.perform(get(JOBS_URL + "/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.filePath").value("uploads/customers.csv"));
    }

    @Test
    void returnsNotFoundForUnknownJobAndBadRequestForMalformedId() throws Exception {
        mockMvc.perform(get(JOBS_URL + "/" + UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));

        mockMvc.perform(get(JOBS_URL + "/not-a-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void rejectsInvalidCreateRequest() throws Exception {
        mockMvc.perform(post(JOBS_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"fileName":" "}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors.fileName").exists());
    }

    @Test
    void rejectsMissingAndOversizedFileNames() throws Exception {
        mockMvc.perform(post(JOBS_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors.fileName").exists());

        mockMvc.perform(post(JOBS_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"fileName\":\"" + "x".repeat(256) + "\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors.fileName").exists());
    }

    @Test
    void listsJobsInPages() throws Exception {
        createJob("first.csv");
        createJob("second.csv");

        mockMvc.perform(get(JOBS_URL).param("page", "0").param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(1)))
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.totalPages").value(2));
    }

    @Test
    void usesDefaultPaginationWhenParametersAreOmitted() throws Exception {
        createJob("first.csv");
        createJob("second.csv");

        mockMvc.perform(get(JOBS_URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.number").value(0))
                .andExpect(jsonPath("$.size").value(20))
                .andExpect(jsonPath("$.content", hasSize(2)));
    }

    @Test
    void rejectsPageSizesOutsideSupportedRange() throws Exception {
        mockMvc.perform(get(JOBS_URL).param("size", "101"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors").isNotEmpty());

        mockMvc.perform(get(JOBS_URL).param("size", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors").isNotEmpty());

        mockMvc.perform(get(JOBS_URL).param("page", "-1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors").isNotEmpty());
    }

    private void createJob(String fileName) throws Exception {
        mockMvc.perform(post(JOBS_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"fileName\":\"" + fileName + "\"}"))
                .andExpect(status().isCreated());
    }
}
