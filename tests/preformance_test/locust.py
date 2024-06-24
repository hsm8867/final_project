from locust import HttpUser, task, between


class PerformanceTests(HttpUser):
    wait_time = between(1, 3)

    @task(1)
    def test_fastapi(self):
        response = self.client.get("/")
        if response.status_code == 200:
            print(response.json())
        else:
            print(f"Failed with status code: {response.status_code}")

    @task(2)
    def test_predict(self):
        response = self.client.post("/predict", json={"moviename": "Inception"})
        if response.status_code == 200:
            print(response.json())
        else:
            print(f"Failed with status code: {response.status_code}")

    @task(3)
    def test_movie_service(self):
        response = self.client.get(
            "/movies_list", params={"date": "2023-12-31", "page": 1, "limit": 10}
        )
        if response.status_code == 200:
            print(response.json())
        else:
            print(f"Failed with status code: {response.status_code}")
