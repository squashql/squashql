package io.squashql;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.squashql.store.Store;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;

import java.sql.Date;
import java.time.Duration;
import java.util.Map;

import static io.squashql.BigQueryServiceAccountDatastore.fetchStoresByName;

/**
 * Implementation of {@link BigQueryDatastore} that uses with end user authentication to leverage fine-grained access
 * provided by BigQuery.
 */
public class BigQueryEndUserDatastore implements BigQueryDatastore {

  private final OAuth2AuthorizedClientRepository authorizedClientRepository;
  /**
   * Use a cache not to execute multiple requests when executing queries.
   */
  private final Cache<String, Map<String, Store>> stores;
  private final String projectId;
  private final String datasetName;

  public BigQueryEndUserDatastore(OAuth2AuthorizedClientRepository authorizedClientRepository, String projectId, String datasetName) {
    this.authorizedClientRepository = authorizedClientRepository;
    this.projectId = projectId;
    this.datasetName = datasetName;
    this.stores = Caffeine.newBuilder()
            .maximumSize(16)
            .expireAfterWrite(Duration.ofMinutes(5))
            .build();
  }

  @Override
  public String getProjectId() {
    return this.projectId;
  }

  @Override
  public String getDatasetName() {
    return this.datasetName;
  }

  @Override
  public BigQuery getBigquery() {
    var token = getOAuth2AuthenticationToken();
    var accessToken = this.authorizedClientRepository
            .loadAuthorizedClient(token.getAuthorizedClientRegistrationId(), token, null)
            .getAccessToken();
    return BigQueryOptions.newBuilder()
            .setCredentials(OAuth2Credentials.create(new AccessToken(accessToken.getTokenValue(), Date.from(accessToken.getExpiresAt()))))
            .setProjectId(this.projectId)
            .build()
            .getService();
  }

  @Override
  public Map<String, Store> storesByName() {
    OAuth2AuthenticationToken token = getOAuth2AuthenticationToken();
    return this.stores.get(token.getPrincipal().getName(), name -> fetchStoresByName(this));
  }

  private OAuth2AuthenticationToken getOAuth2AuthenticationToken() {
    SecurityContext context = SecurityContextHolder.getContext();
    if (context == null) {
      throw new AuthenticationServiceException("not authenticated");
    }

    Authentication authentication = context.getAuthentication();
    if (authentication instanceof OAuth2AuthenticationToken token) {
      return token;
    } else {
      throw new IllegalStateException("authentication type not expected: " + authentication.getClass());
    }
  }
}
