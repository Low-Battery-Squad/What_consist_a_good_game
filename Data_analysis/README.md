# Data Analysis 

After Data_load，we have to connect to the PostgreSQL database through **psycopg2** to obtain all the dataset to run our regression。Please follow these following stuff to go through my work.

## I. Prerequisites

To ensure the analysis environment is complete and ready, the following setup steps are necessary:

1.  **Structure Creation:**
    ```bash
    mkdir Data_analysis
    mkdir Data_analysis/figures
    ```

2.  **Required Dependencies Installation:**
    ```bash
    pip install pandas numpy statsmodels matplotlib seaborn psycopg2-binary python-dotenv
    ```
3.  **Fill up the .env file**

    add your password to .env file  

## II. Regression Model Setup (for the first time)

Establish the OLS model using the statsmodels library, with review_ratio as the dependent variable.

<img width="1019" height="111" alt="image" src="https://github.com/user-attachments/assets/f80ef48a-bf75-438c-b627-a106e2824c6b" />
The genre is set as a dummy variavle. For example, x1-x9 represnet 9 genre of games(including action, RPG.....). If there is an action game is involved, x1(action) = 1, the rest(x2-x9) = 0.

This move is aimed to see whether result fit our hypothetical regression model.


## III. See a problem after running the regression model for the first time

The initial model suffered from **severe multicollinearity** caused by certain genre dummy variables (e.g., `g_Nudity`, `g_Sports`), leading to unstable coefficients and inflated standard errors. 


<img width="806" height="600" alt="image" src="https://github.com/user-attachments/assets/00abbe02-0bd1-4203-aaef-abee6894d3d6" />

Note: x1-x9 represent the different genre of the game: action, sports, .....
The condition number is large, 1.16e+05. This might indicate that there arestrong multicollinearity or other numerical problems.



**Improvement Method:** To obtain robust statistical results, we improved the model by removing the highly collinear and unstable genre variables（g_Nudity、g_Sports）, thereby optimizing model fit and interpretability.

## IV. Results of the Improved Regression Model

After making adjustment to our model:

The following table presents the core results from the optimized model (using HC3 robust standard errors):

| Variable | Coefficient | P Value | Confidence Interval Lower | Confidence Interval Upper |
| :--- | :--- | :--- | :--- | :--- |
| **const** | $0.7625$ | $0.000$ | $0.701$ | $0.824$ |
| **current\_price** | $0.0002$ | $0.828$ | $-0.001$ | $0.002$ |
| **total\_reviews** | $1.677e^{-07}$ | $0.001$ | $6.95e^{-08}$ | $2.66e^{-07}$ |
| **days\_since\_release**| $1.203e^{-05}$ | $0.007$ | $3.32e^{-06}$ | $2.07e^{-05}$ |
| **g\_Adventure** | $0.0250$ | $0.168$ | $-0.011$ | $0.061$ |
| **g\_Casual** | $0.0317$ | $0.416$ | $-0.045$ | $0.108$ |
| **g\_Indie** | $0.0103$ | $0.722$ | $-0.046$ | $0.067$ |
| **g\_RPG** | $-0.0474$ | $0.108$ | $-0.105$ | $0.010$ |
| **g\_Racing** | $0.0866$ | $0.000$ | $0.054$ | $0.120$ |
| **g\_Simulation** | $-0.0599$ | $0.272$ | $-0.167$ | $0.047$ |
| **g\_Strategy** | $-0.0228$ | $0.362$ | $-0.072$ | $0.026$ |


## V. What we can learn?

The optimized regression model provides quantitative answers to the following key questions:
1.  **Price Neutrality:** **Game price has no statistically significant impact** on the Review Ratio ($p > 0.9$), holding other factors constant.
2.  **Popularity Dominance:** **Total Reviews (`total_reviews`)** is the strongest positive predictor ($p < 0.001$), indicating that a game's popularity is a deciding factor in its positive review rate.
3.  **Genre Advantage:** **Racing games (`g_Racing`)** exhibit a significant advantage, increasing the Review Ratio by approximately 8.66 percentage points compared to the baseline genre.
