import datetime
import requests
import itertools

import xml.etree.ElementTree as xml

from typing import Union

from .helpers import batches
from .article import PubMedArticle
from .book import PubMedBookArticle


# Base url for all queries
BASE_URL = "https://eutils.ncbi.nlm.nih.gov"


class PubMed(object):
    """ Wrapper around the PubMed API.
    """

    def __init__(
        self: object, tool: str = "my_tool", email: str = "my_email@example.com"
    ) -> None:
        """ Initialization of the object.

            Parameters:
                - tool      String, name of the tool that is executing the query.
                            This parameter is not required but kindly requested by
                            PMC (PubMed Central).
                - email     String, email of the user of the tool. This parameter
                            is not required but kindly requested by PMC (PubMed Central).

            Returns:
                - None
        """

        # Store the input parameters
        self.tool = tool
        self.email = email

        # Keep track of the rate limit
        self._rateLimit = 3
        self._requestsMade = []

        # Define the standard / default query parameters
        self.parameters = {"tool": tool, "email": email, "db": "pubmed"}

    def query(self: object, query: str, max_results: int = 100, timeout: int = 10):
        """ Method that executes a query agains the GraphQL schema, automatically
            inserting the PubMed data loader.

            Parameters:
                - query     String, the GraphQL query to execute against the schema.

            Returns:
                - result    ExecutionResult, GraphQL object that contains the result
                            in the "data" attribute.
        """

        # Retrieve the article IDs for the query
        article_ids = self._getArticleIds(query=query, max_results=max_results, timeout=timeout)

        # Get the articles themselves
        articles = list(
            [
                self._getArticles(article_ids=batch, timeout=timeout)
                for batch in batches(article_ids, 250)
            ]
        )

        # Chain the batches back together and return the list
        return itertools.chain.from_iterable(articles)


    def query_publication_ids(self: object, query: str, max_results: int = 100, timeout: int = 10):
        """ Method that executes a query agains the GraphQL schema, automatically
            inserting the PubMed data loader.

            Parameters:
                - query     String, the GraphQL query to execute against the schema.

            Returns:
                - result    ExecutionResult, GraphQL object that contains the result
                            in the "data" attribute.
        """

        # Retrieve the article IDs for the query
        article_ids = self._getArticleIds(query=query, max_results=max_results, timeout=timeout)

        return article_ids


    def get_publications_from_ids(self: object, article_ids: list, timeout:int =10):
        """ Method that executes a query agains the GraphQL schema, automatically
            inserting the PubMed data loader.

            Parameters:
                - query     String, the GraphQL query to execute against the schema.

            Returns:
                - result    ExecutionResult, GraphQL object that contains the result
                            in the "data" attribute.
        """

        # # Retrieve the article IDs for the query
        # article_ids = self._getArticleIds(query=query, max_results=max_results)

        # Get the articles themselves
        articles = list(
            [
                self._getArticles(article_ids=batch, timeout=timeout)
                for batch in batches(article_ids, 250)
            ]
        )

        # Chain the batches back together and return the list
        return itertools.chain.from_iterable(articles)

    def batch_query(self: object, query: str, batch_size: int = 250, timeout:int = 10):
        # Retrieve the article IDs for the query
        article_ids = self._getArticleIds(query=query, max_results=10000000, timeout=timeout)

        # Get the articles themselves
        for batch in batches(article_ids, batch_size):
            yield list(self._getArticles(article_ids=batch, timeout=timeout))

    def getTotalResultsCount(self: object, query: str, timeout: int = 10) -> int:
        """ Helper method that returns the total number of results that match the query.

            Parameters:
                - query                 String, the query to send to PubMed

            Returns:
                - total_results_count   Int, total number of results for the query in PubMed
        """

        # Get the default parameters
        parameters = self.parameters.copy()

        # Add specific query parameters
        parameters["term"] = query
        parameters["retmax"] = 1

        # Make the request (request a single article ID for this search)
        response = self._get(url="/entrez/eutils/esearch.fcgi", parameters=parameters, timeout=timeout)

        # Get from the returned meta data the total number of available results for the query
        total_results_count = int(response.get("esearchresult", {}).get("count"))

        # Return the total number of results (without retrieving them)
        return total_results_count
    
    def _exceededRateLimit(self) -> bool:
        """ Helper method to check if we've exceeded the rate limit.

            Returns:
                - exceeded      Bool, Whether or not the rate limit is exceeded.
        """

        # Remove requests from the list that are longer than 1 second ago
        self._requestsMade = [requestTime for requestTime in self._requestsMade if requestTime > datetime.datetime.now() - datetime.timedelta(seconds=1)]

        # Return whether we've made more requests in the last second, than the rate limit
        return len(self._requestsMade) > self._rateLimit

    def _get(
        self: object, url: str, parameters: dict, output: str = "json", timeout: int = 10
    ) -> Union[dict, str]:
        """ Generic helper method that makes a request to PubMed.

            Parameters:
                - url           Str, last part of the URL that is requested (will
                                be combined with the base url)
                - parameters    Dict, parameters to use for the request
                - output        Str, type of output that is requested (defaults to
                                JSON but can be used to retrieve XML)

            Returns:
                - response      Dict / str, if the response is valid JSON it will
                                be parsed before returning, otherwise a string is
                                returend
        """

        # Make sure the rate limit is not exceeded
        while self._exceededRateLimit():
            pass

        # Set the response mode
        parameters["retmode"] = output

        # Make the request to PubMed
        response = requests.get(f"{BASE_URL}{url}", params=parameters, timeout=timeout)

        # Check for any errors
        response.raise_for_status()

        # Add this request to the list of requests made
        self._requestsMade.append(datetime.datetime.now())

        # Return the response
        if output == "json":
            return response.json()
        else:
            return response.text

    def _getArticles(self: object, article_ids: list, timeout: int = 10) -> list:
        """ Helper method that batches a list of article IDs and retrieves the content.

            Parameters:
                - article_ids   List, article IDs.

            Returns:
                - articles      List, article objects.
        """

        # Get the default parameters
        parameters = self.parameters.copy()
        parameters["id"] = article_ids

        # Make the request
        response = self._get(
            url="/entrez/eutils/efetch.fcgi", parameters=parameters, output="xml", timeout=timeout
        )

        # Parse as XML
        root = xml.fromstring(response)

        # Loop over the articles and construct article objects
        for article in root.iter("PubmedArticle"):
            yield PubMedArticle(xml_element=article)
        for book in root.iter("PubmedBookArticle"):
            yield PubMedBookArticle(xml_element=book)

    def _getArticleIds(self: object, query: str, max_results: int, timeout: int = 10, min_year: int = 1000,
                       max_year: int = 3000, min_month:int =1, max_month: int= 12, min_day: int=1, max_day:int=31) -> list:
        """ Helper method to retrieve the article IDs for a query.

            Parameters:
                - query         Str, query to be executed against the PubMed database.
                - max_results   Int, the maximum number of results to retrieve.

            Returns:
                - article_ids   List, article IDs as a list.
        """

        ## Novel strateies necessary to keep Query below 10k entries:
        # Idea: Split dataset in batches int(all_results/10.000) + 1; SPlit Year Range into an equal amaount of Time Frames
        # Call FUnction again with set Year Range

        # Create a placeholder for the retrieved IDs
        article_ids = []

        # Get the default parameters
        parameters = self.parameters.copy()

        # Add specific query parameters
        parameters["term"] = query
        parameters["retmax"] = 10000
        parameters["mindate"] =f"{min_year}/{min_month}/{min_day}"
        parameters["maxdate"] =f"{max_year}/{max_month}/{max_day}"

        # Calculate a cut off point based on the max_results parameter
        if max_results < parameters["retmax"]:
            parameters["retmax"] = max_results

        # Make the first request to PubMed
        response = self._get(url="/entrez/eutils/esearch.fcgi", parameters=parameters, timeout=timeout)
        # get amount of total fitting publications
        total_result_count = int(response.get("esearchresult", {}).get("count"))
        print("Year range:", min_year, max_year, "Month range:", min_month, max_month, "Day range:", min_day, max_day,
              "Entries to retrieve:", total_result_count)


        # <editor-fold desc="Description">
        if total_result_count > parameters["retmax"]:
            batch_count = int(total_result_count//parameters["retmax"]) + 1
            if min_year != max_year:

                year_gap = max_year - min_year
                year_step = year_gap // batch_count
                year_boundaries = {min_year + (year_step * i) for i in range(batch_count)}
                for i in sorted(year_boundaries):
                    article_ids += self._getArticleIds(query=query, max_results=max_results, timeout=timeout,
                                                  min_year=i if i==min_year else i +1, max_year=i + year_step)
                if isinstance(year_gap/batch_count, float):
                    article_ids += self._getArticleIds(query=query, max_results=max_results, timeout=timeout,
                                                       min_year=max_year, max_year=max_year)

                return article_ids
            else:
                if min_month != min_month:
                    month_gap = max_month - min_month
                    month_step = month_gap // batch_count
                    month_boundaries = {min_month + (month_step * i) for i in range(batch_count)}
                    for i in sorted(month_boundaries):
                        article_ids += self._getArticleIds(query=query, max_results=max_results, timeout=timeout,
                                                      min_year=min_year, max_year=max_year,
                                                      min_month=i if i==min_month else i +1, max_month=i + month_step)
                    if isinstance(month_gap / batch_count, float):
                        article_ids += self._getArticleIds(query=query, max_results=max_results, timeout=timeout,
                                                           min_year=min_year, max_year=max_year,
                                                           min_month=max_month, max_month=max_month)
                    return article_ids
                else:
                    if min_day != min_day:
                        day_gap = max_day - min_day
                        day_step = day_gap// batch_count
                        day_boundaries = {min_day + (day_step * i) for i in range(batch_count)}
                        for i in sorted(day_boundaries):
                            article_ids += self._getArticleIds(query=query, max_results=max_results, timeout=timeout,
                                                          min_year=min_year, max_year=max_year,
                                                          min_month=min_month, max_month=max_month,
                                                          min_day=i if i==min_day else i +1, max_day=i + day_step)
                        if isinstance(day_gap / batch_count, float):
                            article_ids += self._getArticleIds(query=query, max_results=max_results,
                                                               timeout=timeout,
                                                               min_year=min_year, max_year=max_year,
                                                               min_month=min_month, max_month=max_month,
                                                               min_day=max_day, max_day=max_day)
                        return article_ids
                    else:
                        print("Year range:", min_year, max_year, "Month range:", min_month, max_month, "Day range:",
                              min_day, max_day, "Too many entries:", total_result_count)
                        return article_ids # if more than 10.000 entries are given per day, they can not be retrieved!
            # YYYY / MM / DD, and these variants are also allowed: YYYY, YYYY / MM.
        # </editor-fold>

        # Add the retrieved IDs to the list
        article_ids += response.get("esearchresult", {}).get("idlist", [])
        return article_ids

        # # Get information from the response
        # retrieved_count = int(response.get("esearchresult", {}).get("retmax")) -1
        #
        # # If no max is provided (-1) we'll try to retrieve everything
        # if max_results == -1:
        #     max_results = total_result_count
        #
        # # If not all articles are retrieved, continue to make requests untill we have everything
        # while retrieved_count < total_result_count and retrieved_count < max_results:
        #
        #     # Calculate a cut off point based on the max_results parameter
        #     if (max_results - retrieved_count) < parameters["retmax"]:
        #         parameters["retmax"] = max_results - retrieved_count
        #
        #     # Start the collection from the number of already retrieved articles
        #     parameters["retstart"] = retrieved_count
        #
        #     # Make a new request
        #     response = self._get(
        #         url="/entrez/eutils/esearch.fcgi", parameters=parameters
        #     )
        #
        #     # Add the retrieved IDs to the list
        #     article_ids += response.get("esearchresult", {}).get("idlist", [])
        #
        #     # Get information from the response
        #     retrieved_count += int(response.get("esearchresult", {}).get("retmax"))
        #
        # # Return the response
        # return article_ids
